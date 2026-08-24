(ns jackdaw.failure-integration-test
  "Raw Kafka mock tests for client and admin failure paths."
  (:require [clojure.test :refer [deftest is testing]])
  (:import (java.time Duration)
           (java.util Collections)
           (java.util.concurrent ExecutionException Future)
           (org.apache.kafka.clients.admin AlterConfigOp AlterConfigOp$OpType
                                             AlterConfigsResult ConfigEntry
                                             CreatePartitionsOptions
                                             MockAdminClient NewPartitions)
           (org.apache.kafka.clients.consumer CommitFailedException
                                               ConsumerRebalanceListener MockConsumer
                                               OffsetResetStrategy)
           (org.apache.kafka.clients.producer MockProducer ProducerRecord)
           (org.apache.kafka.common KafkaException TopicPartition)
           (org.apache.kafka.common.config ConfigResource ConfigResource$Type)
           (org.apache.kafka.common.errors InvalidRequestException)
           (org.apache.kafka.common.serialization StringSerializer)))

(set! *warn-on-reflection* true)

(defn future-failure
  [^Future future]
  (try
    (.get future)
    nil
    (catch ExecutionException e
      (.getCause e))))

(deftest transactional-send-failure-can-be-aborted
  (let [serializer (StringSerializer.)
        producer (MockProducer. true nil serializer serializer)
        record (ProducerRecord. "events" "key" "value")
        send-failure (KafkaException. "injected send failure")]
    (.initTransactions producer)
    (.beginTransaction producer)
    (set! (.-sendException producer) send-failure)
    (is (thrown-with-msg? KafkaException
                          #"injected send failure"
                          (.send producer record)))
    (.abortTransaction producer)
    (is (.transactionAborted producer))
    (is (not (.transactionInFlight producer)))
    (is (empty? (.history producer)))))

(deftest consumer-commit-failure-is-returned-by-mock-consumer
  (let [failure (CommitFailedException. "injected commit failure")
        consumer (proxy [MockConsumer] [OffsetResetStrategy/EARLIEST]
                   (commitSync
                     ([^Duration _]
                      (throw failure))))]
    (is (thrown-with-msg? CommitFailedException
                          #"injected commit failure"
                          (.commitSync consumer (Duration/ofSeconds 1))))))

(deftest mock-consumer-can-drive-rebalance-during-processing
  (let [consumer (MockConsumer. OffsetResetStrategy/EARLIEST)
        partition (TopicPartition. "events" 0)
        events (atom [])
        listener (reify ConsumerRebalanceListener
                   (onPartitionsRevoked [_ partitions]
                     (swap! events conj [:revoked (vec partitions)]))
                   (onPartitionsAssigned [_ partitions]
                     (swap! events conj [:assigned (vec partitions)]))
                   (onPartitionsLost [_ partitions]
                     (swap! events conj [:lost (vec partitions)])))]
    (.subscribe consumer (Collections/singleton "events") listener)
    (.rebalance consumer (Collections/singleton partition))
    (swap! events conj [:processing (.assignment consumer)])
    (.rebalance consumer Collections/EMPTY_LIST)
    (is (= [:assigned :processing :revoked :assigned]
           (mapv first @events)))
    (is (= [partition]
           (second (first @events))))
    (is (= #{partition}
           (second (second @events))))
    (is (= [partition]
           (second (nth @events 2))))
    (is (= #{ } (.assignment consumer)))))

(deftest mock-admin-partition-expansion-is-explicitly-unimplemented
  (let [admin (MockAdminClient.)
        expansion {"events" (NewPartitions/increaseTo 2)}]
    (testing "Kafka's MockAdminClient does not provide a partition-expansion simulation"
      (is (thrown-with-msg? UnsupportedOperationException
                            #"Not implemented yet"
                            (.createPartitions admin expansion
                                               (CreatePartitionsOptions.)))))))

(deftest mock-admin-returns-config-change-error
  (let [admin (MockAdminClient.)
        resource (ConfigResource. ConfigResource$Type/BROKER "99")
        operation (AlterConfigOp.
                   (ConfigEntry. "log.retention.ms" "60000")
                   AlterConfigOp$OpType/SET)
        ^AlterConfigsResult result (.incrementalAlterConfigs
                                    admin {resource [operation]}
                                    (org.apache.kafka.clients.admin.AlterConfigsOptions.))]
    (is (instance? InvalidRequestException
                   (future-failure (get (.values result) resource))))))
