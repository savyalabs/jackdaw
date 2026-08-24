(ns jackdaw.streams-options-test
  (:require [clojure.test :refer [deftest is testing]]
            [jackdaw.streams :as k]
            [jackdaw.streams.interop :as interop]
            [jackdaw.streams.mock :as mock])
  (:import [java.time Duration]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.streams StreamsBuilder TopologyTestDriver]
            [org.apache.kafka.streams.kstream Consumed Grouped Joined Materialized
            KGroupedStream KStream KTable KeyValueMapper Named Predicate Produced
            Repartitioned StreamJoined ValueJoiner]))

(set! *warn-on-reflection* true)

(deftest option-builders
  (testing "exposes every Kafka Streams DSL option builder"
    (doseq [[builder option-class]
            [[interop/named Named]
             [interop/materialized Materialized]
             [interop/grouped Grouped]
             [interop/joined Joined]
             [interop/stream-joined StreamJoined]
             [interop/repartitioned Repartitioned]
             [interop/produced Produced]
             [interop/consumed Consumed]]]
      (is (instance? option-class (builder {:name "configured"})))))

  (testing "applies option names and materialized stores to a topology"
    (let [key-serde (Serdes/Long)
          value-serde (Serdes/Long)
          builder (interop/streams-builder)
          ^StreamsBuilder raw-builder (k/streams-builder* builder)
          ^KStream left (.stream raw-builder "configured-input"
                                  ^Consumed (interop/consumed {:key-serde key-serde
                                                               :value-serde value-serde
                                                               :name "source-name"}))
          ^KStream right (.stream raw-builder "join-input"
                                   ^Consumed (interop/consumed {:key-serde key-serde
                                                                :value-serde value-serde
                                                                :name "right-source-name"}))
          ^KTable join-table (.table raw-builder "join-table"
                                     ^Consumed (interop/consumed {:key-serde key-serde
                                                                  :value-serde value-serde
                                                                  :name "table-source-name"})
                                     ^Materialized (interop/materialized {:store-name "join-table-store"
                                                                           :key-serde key-serde
                                                                           :value-serde value-serde}))
          ^KStream named (.filter left
                                   (reify Predicate
                                     (test [_ _ _] true))
                                   ^Named (interop/named {:name "filter-name"}))
          ^KGroupedStream grouped (.groupBy named
                                            (reify KeyValueMapper
                                              (apply [_ key _] key))
                                            ^Grouped (interop/grouped {:key-serde key-serde
                                                                       :value-serde value-serde
                                                                       :name "group-name"}))
          ^KTable table (.count grouped
                                 ^Materialized (interop/materialized {:store-name "configured-store"
                                                                       :key-serde key-serde
                                                                       :value-serde value-serde}))
          ^KStream joined (.join left join-table
                                 (reify ValueJoiner
                                   (apply [_ left-value right-value]
                                     (or left-value right-value)))
                                 ^Joined (interop/joined {:key-serde key-serde
                                                          :value-serde value-serde
                                                          :other-value-serde value-serde
                                                          :name "join-name"}))
          ^KStream stream-joined (.join left right
                                        (reify ValueJoiner
                                          (apply [_ left-value right-value]
                                            (or left-value right-value)))
                                        (org.apache.kafka.streams.kstream.JoinWindows/ofTimeDifferenceWithNoGrace
                                         (Duration/ofSeconds 1))
                                        ^StreamJoined (interop/stream-joined {:key-serde key-serde
                                                                               :value-serde value-serde
                                                                               :other-value-serde value-serde
                                                                               :name "stream-join-name"
                                                                               :store-name "stream-join-store"}))
          ^KStream repartitioned (.repartition left
                                                ^Repartitioned (interop/repartitioned {:key-serde key-serde
                                                                                        :value-serde value-serde
                                                                                        :name "repartition-name"}))]
      (.toStream table ^Named (interop/named {:name "table-stream-name"}))
      (.to named "named-output"
           ^Produced (interop/produced {:key-serde key-serde
                                        :value-serde value-serde
                                        :name "sink-name"}))
      (.to joined "joined-output"
           ^Produced (interop/produced {:key-serde key-serde :value-serde value-serde}))
      (.to stream-joined "stream-joined-output"
           ^Produced (interop/produced {:key-serde key-serde :value-serde value-serde}))
      (.to repartitioned "repartitioned-output"
           ^Produced (interop/produced {:key-serde key-serde :value-serde value-serde}))
      (let [^org.apache.kafka.streams.Topology topology (.build raw-builder)
            description (str (.describe topology))]
        (with-open [^TopologyTestDriver driver (mock/topology->test-driver topology)]
          (is (instance? TopologyTestDriver driver)))
        (doseq [configured-name ["source-name" "filter-name" "group-name"
                                 "join-name" "stream-join-name" "repartition-name"
                                 "sink-name"]]
          (is (.contains description configured-name)))
        (doseq [store-name ["configured-store" "join-table-store" "stream-join-store"]]
          (is (.contains description store-name)))))))

(deftest option-builder-fluent-options
  (let [key-serde (Serdes/Long)
        value-serde (Serdes/Long)]
    (is (instance? Joined
                   (interop/joined {:key-serde key-serde
                                    :value-serde value-serde
                                    :other-value-serde value-serde
                                    :name "join-name"
                                    :grace-period (Duration/ofSeconds 1)})))
    (is (instance? StreamJoined
                   (interop/stream-joined {:key-serde key-serde
                                           :value-serde value-serde
                                           :other-value-serde value-serde
                                           :name "stream-join-name"
                                           :store-name "stream-join-store"})))
    (is (instance? Repartitioned
                   (interop/repartitioned {:key-serde key-serde
                                           :value-serde value-serde
                                           :name "repartition-name"
                                           :number-of-partitions 2})))
    (is (instance? Produced
                   (interop/produced {:key-serde key-serde
                                     :value-serde value-serde
                                     :name "produced-name"})))
    (is (instance? Consumed
                   (interop/consumed {:key-serde key-serde
                                     :value-serde value-serde
                                     :name "consumed-name"})))))
