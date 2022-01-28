;; The Climate Corporation licenses this file to you under under the Apache
;; License, Version 2.0 (the "License"); you may not use this file except in
;; compliance with the License.  You may obtain a copy of the License at
;;
;;   http://www.apache.org/licenses/LICENSE-2.0
;;
;; See the NOTICE file distributed with this work for additional information
;; regarding copyright ownership.  Unless required by applicable law or agreed
;; to in writing, software distributed under the License is distributed on an
;; "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
;; or implied.  See the License for the specific language governing permissions
;; and limitations under the License.
(ns com.climate.squeedo.sqs-consumer
  "Functions for using Amazon Simple Queueing Service to request and perform
  computation."
  (:require
   [clojure.core.async :as async :refer [close! go-loop thread >! <! <!! chan buffer timeout]]
   [clojure.tools.logging :as log]
   [com.climate.squeedo.sqs :as sqs]))

(defn- onto-chan-ret!
  "like core.async/onto-chan! except that
   a) not closing ch and
   b) value of returning channel is true unless ch is already closed."
  [ch coll]
  (go-loop [vs (seq coll)]
    (if vs
      (if (>! ch (first vs))
        (recur (next vs))
        ;; closed
        false)
      ;; all pushed.
      true)))

(defn- wait-all-close
  "Returns a channel that closes when all the given channels have closed."
  [chans]
  (let [co (async/chan 1 (filter nil?))]
    (async/pipe (async/merge chans)
                co)
    co))

(defn- close-on-close!
  "Close `ch-to-close` once `watch-ch` is closed."
  [watch-ch ch-to-close]
  (async/go-loop []
    (if (nil? (<! watch-ch))
      (close! ch-to-close)
      (recur))))

(defn- dequeue [connection dequeue-limit]
  (try
    [nil (sqs/dequeue* connection :limit dequeue-limit)]
    (catch Throwable t
      [t nil])))

(defn- queue-listener [connection dequeue-limit exceptional-poll-delay-ms message-chan is-stopped-fn]
  (go-loop []
    (let [[err messages] (dequeue connection dequeue-limit)]
      (if err
        (if (is-stopped-fn)
          (log/error err "Encountered exception dequeueing.")
          (do
            (log/errorf err "Encountered exception dequeueing.  Waiting %d ms before trying again." exceptional-poll-delay-ms)
            (<! (timeout exceptional-poll-delay-ms))
            (recur)))
        (when (and (<! (onto-chan-ret! message-chan messages))
                   (not (is-stopped-fn)))
          (recur))))))

(defn- queue-listener-pool [num-listeners create-listener-fn]
  (-> (repeatedly num-listeners
                  create-listener-fn)
      (vec)
      (wait-all-close)))

(defn- create-queue-helper [buffer-size num-listeners create-listener-fn]
  (let [stopped?_ (atom false)
        is-stopped-fn (fn []
                        @stopped?_)
        stop-fn (fn []
                  (reset! stopped?_ true))
        buf (buffer buffer-size)
        message-chan (chan buf)
        listener-done-chan (queue-listener-pool num-listeners
                                                (fn []
                                                  (create-listener-fn message-chan
                                                                      is-stopped-fn)))]
    ;; close message-chan when all listeners are returned.
    (close-on-close! listener-done-chan message-chan)
    [message-chan buf stop-fn]))

(defn- create-queue-listeners
  "Kick off listeners in the background that eagerly grab messages as quickly as possible and fetch them into a buffered
  channel.

  This will park the thread if the message channel is full. (ie. don't prefetch too many messages as there is
  a memory impact, and they have to get processed before timing out.)

  If there is an Exception while trying to poll for messages, wait exceptional-poll-delay-ms before trying again."
  [connection num-listeners buffer-size dequeue-limit exceptional-poll-delay-ms]
  (create-queue-helper buffer-size
                       num-listeners
                       (fn [message-chan is-stopped-fn]
                         (queue-listener connection dequeue-limit exceptional-poll-delay-ms message-chan is-stopped-fn))))

(defn- dedicated-queue-listener [connection dequeue-limit exceptional-poll-delay-ms message-chan is-stopped-fn]
  (let [finish-chan (chan)]
    (thread
      (loop []
        (let [[err messages] (dequeue connection dequeue-limit)]
          (if err
            (if (is-stopped-fn)
              (log/error err "Encountered exception dequeueing.")
              (do
                (log/errorf err "Encountered exception dequeueing.  Waiting %d ms before trying again." exceptional-poll-delay-ms)
                (Thread/sleep exceptional-poll-delay-ms)
                (recur)))
            (when (and (<!! (onto-chan-ret! message-chan messages))
                       (not (is-stopped-fn)))
              (recur)))))
      (close! finish-chan))
    finish-chan))

(defn- create-dedicated-queue-listeners
  "Similar to `create-queue-listeners` but listeners are created on dedicated threads and will be blocked when the
  message channel is full.  Potentially useful when consuming from large numbers of SQS queues within a single program.

  If there is an Exception while trying to poll for messages, wait exceptional-poll-delay-ms before trying again."
  [connection num-listeners buffer-size dequeue-limit exceptional-poll-delay-ms]
  (create-queue-helper buffer-size
                       num-listeners
                       (fn [message-chan is-stopped-fn]
                         (dedicated-queue-listener connection dequeue-limit exceptional-poll-delay-ms message-chan is-stopped-fn))))

(defn- worker
  [work-token-chan message-chan compute done-chan]
  (go-loop []
    (>! work-token-chan :token)
    (when-let [message (<! message-chan)]
      (let [res-chan (chan 1)]
        (try
          (compute message res-chan)
          (catch Throwable _
            (>! res-chan (assoc message :nack true))))
        (>! done-chan res-chan))
      (recur))))

(defn- worker-pool
  "Returns a channel that closes when all workers are returned.
  It may not mean that all jobs are done, as a worker may dispatch a asynchronous job."
  [worker-size work-token-chan message-chan compute done-chan]
  (-> (repeatedly worker-size
                  (fn []
                    (worker work-token-chan message-chan compute done-chan)))
      (vec)
      (wait-all-close)))

(defn- acker
  [connection work-token-chan done-chan]
  (go-loop []
    (when-let [message-ch (<! done-chan)]
      ;; free up the work-token-chan
      (<! work-token-chan)
      ;; (n)ack the message
      (when-let [message (<! message-ch)]
        (let [nack (:nack message)]
          (cond
            (integer? nack) (sqs/nack connection message (:nack message))
            nack            (sqs/nack connection message)
            :else           (sqs/ack connection message)))
        (close! message-ch))
      (recur))))

(defn- acker-pool
  "Returns a channel that closes when all jobs pushed to `done-chan` are reported to SQS."
  [connection worker-size work-token-chan done-chan]
  (-> (repeatedly worker-size
                  (fn []
                    (acker connection work-token-chan done-chan)))
      (vec)
      (wait-all-close)))

(defn- create-workers
  "Create workers to run the compute function. Workers are expected to be CPU bound or handle all IO in an asynchronous
  manner. "
  [connection worker-size max-concurrent-work message-chan compute]
  (let [done-chan (chan worker-size)
        ;; the work-token-channel ensures we only have a fixed numbers of messages processed at one time
        work-token-chan (chan max-concurrent-work)
        worker-done-chan (worker-pool worker-size work-token-chan message-chan compute done-chan)
        ack-done-chan (acker-pool connection worker-size work-token-chan done-chan)]
    ;; close done-chan when all workers are returned.
    (close-on-close! worker-done-chan done-chan)
    {:done-chan done-chan
     :ack-done-chan ack-done-chan}))

(defn- dedicated-worker [message-chan compute done-chan]
  (let [finish-chan (chan)]
    (thread
      (loop []
        (when-let [message (<!! message-chan)]
          (let [res-chan (chan 1)]
            (try
              (compute message res-chan)
              (catch Throwable _
                (async/>!! res-chan (assoc message :nack true))))
            ;; may use put! not to block here. but what's the good point of processing another jobs when reporting is delaying.
            (async/>!! done-chan res-chan))
          (recur)))
      (close! finish-chan))
    finish-chan))

(defn- dedicated-worker-pool [worker-size message-chan compute done-chan]
  (-> (repeatedly worker-size
                  (fn []
                    (dedicated-worker message-chan compute done-chan)))
      (vec)
      (wait-all-close)))

(defn- dedicated-acker
  [connection done-chan]
  (let [finish-chan (chan)]
    (thread
      (loop []
        (when-let [message-ch (async/<!! done-chan)]
          ;; (n)ack the message
          (when-let [message (async/<!! message-ch)]
            (let [nack (:nack message)]
              (cond
                (integer? nack) (sqs/nack connection message (:nack message))
                nack            (sqs/nack connection message)
                :else           (sqs/ack connection message)))
            (close! message-ch))
          (recur)))
      (close! finish-chan))
    finish-chan))

(defn- dedicated-acker-pool
  "Returns a channel that closes when all jobs pushed to `done-chan` are reported to SQS."
  [connection worker-size done-chan]
  (-> (repeatedly worker-size
                  (fn []
                    (dedicated-acker connection done-chan)))
      (vec)
      (wait-all-close)))

(defn- create-dedicated-workers
  "Create workers to run the compute function. It does not respect `worker-size` parameter and only respect `max-concurrent-work` for implemetation simplicity"
  [connection worker-count max-concurrent-work message-chan compute]
  (let [done-chan (chan worker-count)
        worker-done-chan (dedicated-worker-pool max-concurrent-work message-chan compute done-chan)
        ;; TODO: convert back to non-dedicated-acker-pool
        acker-pool-size 2
        ack-done-chan (dedicated-acker-pool connection acker-pool-size done-chan)]
    ;; close done-chan when all workers are returned.
    (close-on-close! worker-done-chan done-chan)
    {:done-chan done-chan
     :ack-done-chan ack-done-chan}))

(defn- ->options-map
  "If options are provided as a map, return it as-is; otherwise, the options are provided as varargs and must be
  converted to a map"
  [options]
  (if (= 1 (count options))
    ; if only 1 option, assume it is a map, otherwise options are provided in varargs pairs
    (first options)
    ; convert the varags list into a map
    (apply hash-map options)))

(defn- get-available-processors
  []
  (.availableProcessors (Runtime/getRuntime)))

(defn- get-worker-count
  [options]
  (or (:num-workers options)
      (max 1 (- (get-available-processors) 1))))

(defn- get-listener-count
  [worker-count options]
  (or (:num-listeners options)
      (max 1 (int (/ worker-count 10)))))

(defn- get-listener-threads
  [options]
  (or (:listener-threads? options) false))

(defn- get-message-channel-size
  [listener-count options]
  (or (:message-channel-size options)
      (* 20 listener-count)))

(defn- get-max-concurrent-work
  [worker-count options]
  (or (:max-concurrent-work options)
      worker-count))

(defn- get-dequeue-limit
  [options]
  (or (:dequeue-limit options)
      10))

(defn- get-exceptional-poll-delay-ms
  [options]
  (or (:exceptional-poll-delay-ms options)
      10000))

(defn- dead-letter-deprecation-warning
  [options]
  (when (:dl-queue-name options)
    (println
      (str "WARNING - :dl-queue-name option for com.climate.squeedo.sqs-consumer/start-consumer"
           " has been removed. Please use com.climate.squeedo.sqs/configure to configure an SQS"
           " dead letter queue."))))

(defn- get-worker-threads
  [options]
  (or (:worker-threads? options) false))

(defn start-consumer
  "Creates a consumer that reads messages as quickly as possible into a local buffer up
   to the configured buffer size.

   Work is done asynchronously by workers controlled by the size of the work buffer (currently
   hardcoded to number of cpus minus 1) Bad things happen if you have workers > number of cpus.

   The client has responsibility for calling stop-consumer when you no longer want to process messages.
   Additionally, the client MUST send the message back on the done-channel when processing is complete
   or if an uncaught exception occurs the message will be auto-nack'd in SQS.

   Failed messages will currently not be acked, but rather go back on for redelivery after the timeout.

   Input:
    queue-name - the name of an sqs queue
    compute - a compute function that takes two args: a 'message' containing the body of the sqs
              message and a channel on which to ack/nack when done.

   Optional arguments:
    :message-channel-size      - the number of messages to prefetch from sqs; default 20 * num-listeners
    :num-workers               - the number of workers processing messages concurrently
    :num-listeners             - the number of listeners polling from sqs; default is (num-workers / 10) because each
                                 listener dequeues up to 10 messages at a time
    :listener-threads?         - run listeners in dedicated threads; if true, will create one thread per listener
    :dequeue-limit             - the number of messages to dequeue at a time; default 10
    :max-concurrent-work       - the maximum number of total messages processed.  This is mainly for async workflows;
                                 default num-workers
    :client                    - the SQS client to use (if missing, sqs/mk-connection will create a client)
    :exceptional-poll-delay-ms - when an Exception is received while polling, the number of ms we wait until polling
                                 again.  Default is 10000 (10 seconds).
    :worker-threads?           - run workers in dedicated threads; if true, will create one thread per worker
   Output:
    a map with keys, :done-channel      - the channel to send messages to be acked
                     :message-channel   - unused by the client.
                     :ack-done-channel  - the channel closes when all messages are acked.
  "
  [queue-name compute & opts]
  (let [options (->options-map opts)
        _ (dead-letter-deprecation-warning options)
        connection (sqs/mk-connection queue-name :client (:client options))
        worker-count (get-worker-count options)
        listener-count (get-listener-count worker-count options)
        message-chan-size (get-message-channel-size listener-count options)
        max-concurrent-work (get-max-concurrent-work worker-count options)
        dequeue-limit (get-dequeue-limit options)
        exceptional-poll-delay-ms (get-exceptional-poll-delay-ms options)
        [message-chan _ stop-listener-fn]
        (if (get-listener-threads options)
          (create-dedicated-queue-listeners
           connection listener-count message-chan-size dequeue-limit exceptional-poll-delay-ms)
          (create-queue-listeners
           connection listener-count message-chan-size dequeue-limit exceptional-poll-delay-ms))
        worker-chans (if (get-worker-threads options)
                       (create-dedicated-workers
                        connection worker-count max-concurrent-work message-chan compute)
                       (create-workers
                        connection worker-count max-concurrent-work message-chan compute))]
    {:done-channel (:done-chan worker-chans)
     :ack-done-channel (:ack-done-chan worker-chans)
     :connection connection
     :message-channel message-chan
     :stop-listener-fn stop-listener-fn}))

(defn stop-consumer
  "Takes a consumer created by start-consumer and closes the channels.
  This should be called to stop consuming messages.
  Returns a channel that will close when all ongoing jobs are finished."
  [{:keys [ack-done-channel connection stop-listener-fn]}]
  (stop-listener-fn)
  (async/go
    (<! ack-done-channel)
    (sqs/shutdown-default-client connection)))

(defn stop-consumer!!
  "Block until consumer stopped"
  [consumer]
  (async/<!! (stop-consumer consumer)))

(defn graceful-stop-consumer!
  "Takes a consumer created by start-consumer and tries to stop it.
  Wait at most `timeout-ms` until the consumer has come to a complete stop.
  Returns the result (:timed-out or :finished)."
  [consumer timeout-ms]
  (let [timeout-ch (async/timeout timeout-ms)
        stopped-ch (stop-consumer consumer)]
    (async/go
      (async/alt!
        timeout-ch :timed-out
        stopped-ch :finished))))

(defn graceful-stop-consumer!!
  "like graceful-stop-consumer!, but blocking way."
  [consumer timeout-ms]
  (let [timeout-ch (async/timeout timeout-ms)
        stopped-ch (stop-consumer consumer)]
    (async/alt!!
      timeout-ch :timed-out
      stopped-ch :finished)))
