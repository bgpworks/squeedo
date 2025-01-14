## 1.1.7 (Jan 31, 2022)
* Fix test code.
* Fix regression bug: worker-pool did not respect `worker-count` for async jobs.
* Fix bug: an exception from (n)ack blocks whole consumer.
* Perf: A slow worker does not block `com.climate.squeedo.sqs-consumer/acker`.

## 1.1.6 (Jan 24, 2022)
* **BREAKING**
  * delete `com.climate.squeedo.sqs-consumer/graceful-stop-consumer`. Use `com.climate.squeedo.sqs-consumer/graceful-stop-consumer!` or `com.climate.squeedo.sqs-consumer/graceful-stop-consumer!!` instead.

## 1.1.5 (Jan 23, 2022)
* `com.climate.squeedo.sqs-consumer/stop-consumer` waits internal SQS message listeners to finish ongoing polling to avoid message loss. It also ensures that the underlying SQS client is not used anymore so that the caller can safely close the SQS client.

## 1.1.4 (Jan 20, 2022)

* **BREAKING**
  * `(:done-channel (com.climate.squeedo.sqs-consumer/start-consumer))` now returns a channel contains message-returning channels instead of messages.
* Add `com.climate.squeedo.sqs-consumer/graceful-stop-consumer` to support graceful shutdown.
* When stopping consumer, destroy an internal SQS client if it was automatically created.
* Introduces new configuration option: `:worker-threads?` - run workers in dedicated threads; if true, will create one thread per worker.
  Calculating worker job and reporting with a blocking SQS client in core.async threads may cause dead lock, as there are only limited number of core.async threads. (default to 8).

## 1.1.3 (Jan 11, 2022)

* update underlying dependencies

## 1.1.2 (May 29, 2019)

* update sqs namespace to use the AmazonSQS interface rather than the implementation class AmazonSqsClient
  to allow injecting other AmazonSQS implementations.

## 1.1.0 (February 26, 2019)

* protect clients from aggressive consumer dequeueing when the SQS client returns an exception.
  introduces new configuration option, `:exceptional-poll-delay-ms`, with a new default
  behavior of timing out for 10 seconds if an exception occurs when dequeueing. (thanks @stephencrampton!)
* by default, use `.getStringValue` when clojurifying MessageAttributeValue objects. (thanks @danieltdt!)

## 1.0.2 (September 25, 2018)

* refactor consumer implementation to shorten class file names (#43).

## 1.0.1 (April 24, 2018)

* option to run listeners in dedicated threads instead of in core.async go
  threads (#37).

## 1.0.0 (February 26, 2018)

* no changes from `1.0.0-beta2`.

## 1.0.0-beta2 (December 7, 2017)

* support FIFO message attributes in `com.climate.squeedo.sqs/enqueue` via `:message-group-id` and `:message-deduplication-id`.

## 1.0.0-beta1 (November 20, 2017)

* **BREAKING**
  * queue attributes are only set via `com.climate.squeedo.sqs/configure-queue`
    (replaces `com.climate.squeedo.sqs/set-queue-attrubutes` from 0.2.2).
    * `com.climate.squeedo.sqs/configure-queue` will create the specified
      queue (and dead letter queue) if it does not exist.
    * removed attribute options from `com.climate.squeedo.sqs/mk-connection`.
      this function now only makes a connection and returns a reusable connection object.
      a `QueueDoesNotExistException` exception will be thrown if the queue does not exist
      (for convenience, `com.climate.squeedo.sqs/configure-queue` can be used
      to create the queue first).
    * removed dead letter queue option in `com.climate.squeedo.sqs-consumer/start-consumer`.
      use `com.climate.squeedo.sqs/configure-queue` to set up dead letter queue.
    * removed the default behavior of creating a dead letter queue when starting a
      consumer.

## 0.2.3 (November 20, 2017)

* fix default dead letter queue in SQS consumer when queue is a FIFO queue
* support creating a FIFO queue when connecting to one that does not exist

## 0.2.2 (November 9, 2017)

* support and validate FIFO queue names (thanks @lainiewright!)
* **BREAKING**
  * queue attributes are only set when `com.climate.squeedo.sqs/mk-connection` creates a new sqs queue
    * if an existing queue is found, queue attributes are not applied (this includes dead-letter/redrive configuration)
    * squeedo will emit a `WARN` log when this occurs
    * allows reading from queues that already exist for consumers without create permissions (#34)
    * new api function, `com.climate.squeedo.sqs/set-queue-attributes`, which allows ad-hoc calls to set attributes
    for those who need it
  * support binary message attributes (#30)
  * remove bandalore as a source dependency

## 0.2.1 (June 26, 2017)

* consumer middleware for deserialization and exception logging

## 0.2.0 (June 26, 2017)

* adjustable visibility timeout in consumer api via `:nack timeout`
* updated deps: `[org.clojure/core.async "0.3.442"]`
