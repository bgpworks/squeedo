(ns com.climate.squeedo.sqs-extra
  (:import (com.amazonaws.services.sqs AmazonSQS
                                       AmazonSQSClientBuilder)
           (com.amazonaws.services.sqs.model CreateQueueRequest
                                             DeleteQueueRequest
                                             GetQueueAttributesRequest)))

(defn create-queue
  "Creates a queue with the given name, returning the corresponding URL string.
   Returns successfully if the queue already exists."
  [^AmazonSQS client ^String queue-name]
  (->> (CreateQueueRequest. queue-name)
       (.createQueue client)
       .getQueueUrl))

(defn delete-queue
  "Deletes the queue specified by the given URL string."
  [^AmazonSQS client queue-url]
  (.deleteQueue client (DeleteQueueRequest. queue-url)))

(defn create-client []
  (AmazonSQSClientBuilder/defaultClient))

(defn queue-attrs
  [^AmazonSQS client queue-url]
  (->> (doto (GetQueueAttributesRequest. queue-url)
         (.withAttributeNames #{"All"}))
       (.getQueueAttributes client)
       .getAttributes
       (into {})))
