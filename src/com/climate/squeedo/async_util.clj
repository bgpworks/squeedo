(ns com.climate.squeedo.async-util
  (:require [clojure.core.async :as async]))

(defn onto-chan-ret!
  "like core.async/onto-chan! except that
   a) not closing ch and
   b) value of returning channel is true unless ch is already closed."
  [ch coll]
  (async/go-loop [vs (seq coll)]
    (if vs
      (if (async/>! ch (first vs))
        (recur (next vs))
        ;; closed
        false)
      ;; all pushed.
      true)))

(defn wait-all-close
  "Returns a channel that closes when all the given channels have closed.
  This function consumes all channels in `chans`."
  [chans]
  (let [co (async/chan 1 (filter nil?))]
    (async/pipe (async/merge chans)
                co)
    co))

(defn close-on-close!
  "Close `ch-to-close` once `watch-ch` is closed."
  [watch-ch ch-to-close]
  (async/go-loop []
    (if (nil? (async/<! watch-ch))
      (async/close! ch-to-close)
      (recur))))

(defn auto-closing-chan
  "Returns a channel which will closes after getting an element."
  []
  (async/chan 1 (take 1)))

(defn- removev-elem
  "Returns a vector of the all items from `coll` except `elem`."
  [coll elem]
  (filterv (fn [el]
             (not= el elem))
           coll))

(defn pipes
  "Takes elements from the a channel taken from `from-ch-ch` channel and supplies them to the `to-ch` channel.
  `to-ch` channel will be closed when the `from-ch-ch` channel closes and all channels taken from it also close."
  [from-ch-ch to-ch]
  (let [channels_ (atom [from-ch-ch])]
    (async/go
      (loop []
        (when-let [channels (seq @channels_)]
          (let [[evt-val evt-ch] (async/alts! channels)]
            (cond
              (nil? evt-val)
              (do
                (swap! channels_
                       removev-elem
                       evt-ch)
                (recur))
              
              (= evt-ch from-ch-ch)
              (do
                (swap! channels_
                       conj
                       evt-val)
                (recur))

              :else
              (when (async/>! to-ch evt-val)
                (recur))))))
      (async/close! to-ch))))
