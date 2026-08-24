(ns jackdaw.streams.interop
  "Clojure wrapper to kafka streams."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:refer-clojure :exclude [count map reduce group-by merge filter peek])
  #_{:clj-kondo/ignore [:refer-all]}
  (:require [jackdaw.streams.protocols :refer :all]
            [jackdaw.streams.lambdas :refer :all])
  (:import [java.util
            Collection]
           [java.util.function
            Function Consumer]
           [java.util.regex
            Pattern]
           [java.time
            Duration]
           [org.apache.kafka.streams
            StreamsBuilder]
           [org.apache.kafka.streams.kstream
            Aggregator Branched BranchedKStream Consumed GlobalKTable Grouped
            Initializer Joined StreamJoined
            JoinWindows KGroupedStream KGroupedTable KStream KTable
            KeyValueMapper Materialized Merger Predicate Printed Produced
            Named Reducer Repartitioned SessionWindowedKStream SessionWindows
            Suppressed Suppressed$BufferConfig TimeWindowedKStream ValueJoiner
            ValueMapper Windows ForeachAction]
           [org.apache.kafka.streams.processor.api
            ProcessorSupplier]
           [org.apache.kafka.streams.state Stores]))

(set! *warn-on-reflection* true)

(defn- apply-options
  "Applies present entries in an options map to a Kafka Streams option object.
  Each entry in `option-fns` maps a Clojure option key to a typed fluent
  setter, keeping option-object construction consistent across DSL classes."
  [option options option-fns]
  (clojure.core/reduce (fn [option [key setter]]
                         (let [value (get options key)]
                           (if (some? value)
                             (setter option value)
                             option)))
                       option
                       option-fns))

(defn named
  "Builds a Kafka Streams `Named` from `:name`."
  [{:keys [name]}]
  (Named/as ^String name))

(defn materialized
  "Builds a Kafka Streams `Materialized` from a DSL options map.
  Supports `:store-name`, serde options, logging and caching flags, topic
  configuration, retention, and store suppliers."
  [{:keys [store-name key-serde value-serde] :as options}]
  (let [option (if store-name
                 (Materialized/as ^String store-name)
                 (Materialized/with key-serde value-serde))]
    (apply-options
     option
     options
     [[:key-serde (fn [^Materialized option ^org.apache.kafka.common.serialization.Serde serde]
                    (.withKeySerde option serde))]
      [:value-serde (fn [^Materialized option ^org.apache.kafka.common.serialization.Serde serde]
                      (.withValueSerde option serde))]
      [:logging-config (fn [^Materialized option config]
                         (.withLoggingEnabled option config))]
      [:logging-enabled (fn [^Materialized option enabled]
                          (if enabled
                            (.withLoggingEnabled option {})
                            (.withLoggingDisabled option)))]
      [:caching-enabled (fn [^Materialized option enabled]
                          (if enabled
                            (.withCachingEnabled option)
                            (.withCachingDisabled option)))]
      [:retention (fn [^Materialized option ^Duration retention]
                    (.withRetention option retention))]
      [:store-type (fn [^Materialized option store-type]
                     (.withStoreType option store-type))]])))

(defn grouped
  "Builds a Kafka Streams `Grouped` from DSL options."
  [{:keys [name key-serde value-serde] :as options}]
  (let [option (cond
                 (and key-serde value-serde)
                 (if name
                   (Grouped/with ^String name key-serde value-serde)
                   (Grouped/with key-serde value-serde))
                 name (Grouped/as ^String name)
                 key-serde (Grouped/keySerde key-serde)
                 :else (Grouped/valueSerde value-serde))]
    (apply-options option options
                   [[:name (fn [^Grouped option ^String name] (.withName option name))]
                    [:key-serde (fn [^Grouped option ^org.apache.kafka.common.serialization.Serde serde]
                                  (.withKeySerde option serde))]
                    [:value-serde (fn [^Grouped option ^org.apache.kafka.common.serialization.Serde serde]
                                    (.withValueSerde option serde))]])))

(defn joined
  "Builds a Kafka Streams `Joined` from DSL options."
  [{:keys [name key-serde value-serde other-value-serde] :as options}]
  (let [option (if (and key-serde value-serde other-value-serde)
                 (Joined/with key-serde value-serde other-value-serde)
                 (Joined/as ^String name))]
    (apply-options option options
                   [[:name (fn [^Joined option ^String name] (.withName option name))]
                    [:key-serde (fn [^Joined option ^org.apache.kafka.common.serialization.Serde serde]
                                  (.withKeySerde option serde))]
                    [:value-serde (fn [^Joined option ^org.apache.kafka.common.serialization.Serde serde]
                                    (.withValueSerde option serde))]
                    [:other-value-serde (fn [^Joined option ^org.apache.kafka.common.serialization.Serde serde]
                                          (.withOtherValueSerde option serde))]
                    [:grace-period (fn [^Joined option ^Duration grace]
                                     (.withGracePeriod option grace))]])))

(defn stream-joined
  "Builds a Kafka Streams `StreamJoined` from DSL options."
  [{:keys [name key-serde value-serde other-value-serde] :as options}]
  (let [option (if (and key-serde value-serde other-value-serde)
                 (StreamJoined/with key-serde value-serde other-value-serde)
                 (StreamJoined/as ^String name))]
    (apply-options option options
                   [[:name (fn [^StreamJoined option ^String name] (.withName option name))]
                    [:store-name (fn [^StreamJoined option ^String store-name]
                                   (.withStoreName option store-name))]
                    [:key-serde (fn [^StreamJoined option ^org.apache.kafka.common.serialization.Serde serde]
                                  (.withKeySerde option serde))]
                    [:value-serde (fn [^StreamJoined option ^org.apache.kafka.common.serialization.Serde serde]
                                    (.withValueSerde option serde))]
                    [:other-value-serde (fn [^StreamJoined option ^org.apache.kafka.common.serialization.Serde serde]
                                          (.withOtherValueSerde option serde))]
                    [:logging-config (fn [^StreamJoined option config]
                                       (.withLoggingEnabled option config))]
                    [:logging-enabled (fn [^StreamJoined option enabled]
                                        (if enabled
                                          (.withLoggingEnabled option {})
                                          (.withLoggingDisabled option)))]])))

(defn repartitioned
  "Builds a Kafka Streams `Repartitioned` from DSL options."
  [{:keys [name key-serde value-serde] :as options}]
  (let [option (cond
                 (and key-serde value-serde) (Repartitioned/with key-serde value-serde)
                 name (Repartitioned/as ^String name)
                 :else (Repartitioned/numberOfPartitions 1))]
    (apply-options option options
                   [[:name (fn [^Repartitioned option ^String name] (.withName option name))]
                    [:key-serde (fn [^Repartitioned option ^org.apache.kafka.common.serialization.Serde serde]
                                  (.withKeySerde option serde))]
                    [:value-serde (fn [^Repartitioned option ^org.apache.kafka.common.serialization.Serde serde]
                                    (.withValueSerde option serde))]
                    [:number-of-partitions (fn [^Repartitioned option partitions]
                                             (.withNumberOfPartitions option partitions))]
                    [:partition-fn (fn [^Repartitioned option partition-fn]
                                     (.withStreamPartitioner option (->FnStreamPartitioner partition-fn)))]])))

(defn produced
  "Builds a Kafka Streams `Produced` from DSL options."
  [{:keys [name key-serde value-serde] :as options}]
  (let [option (cond
                 (and key-serde value-serde) (Produced/with key-serde value-serde)
                 name (Produced/as ^String name)
                 :else (Produced/keySerde key-serde))]
    (apply-options option options
                   [[:name (fn [^Produced option ^String name] (.withName option name))]
                    [:key-serde (fn [^Produced option ^org.apache.kafka.common.serialization.Serde serde]
                                  (.withKeySerde option serde))]
                    [:value-serde (fn [^Produced option ^org.apache.kafka.common.serialization.Serde serde]
                                    (.withValueSerde option serde))]
                    [:partition-fn (fn [^Produced option partition-fn]
                                     (.withStreamPartitioner option (->FnStreamPartitioner partition-fn)))]])))

(defn consumed
  "Builds a Kafka Streams `Consumed` from DSL options."
  [{:keys [name key-serde value-serde] :as options}]
  (let [option (cond
                 (and key-serde value-serde) (Consumed/with key-serde value-serde)
                 name (Consumed/as ^String name)
                 :else (Consumed/as ^String name))]
    (apply-options option options
                   [[:name (fn [^Consumed option ^String name] (.withName option name))]
                    [:key-serde (fn [^Consumed option ^org.apache.kafka.common.serialization.Serde serde]
                                  (.withKeySerde option serde))]
                    [:value-serde (fn [^Consumed option ^org.apache.kafka.common.serialization.Serde serde]
                                    (.withValueSerde option serde))]
                    [:timestamp-extractor (fn [^Consumed option extractor]
                                            (.withTimestampExtractor option extractor))]
                    [:offset-reset-policy (fn [^Consumed option ^org.apache.kafka.streams.AutoOffsetReset policy]
                                             (.withOffsetResetPolicy option policy))]])))

(defn topic->consumed
  "Builds a Kafka Streams `Consumed` from a topic config's `:key-serde` and
  `:value-serde`."
  [{:keys [key-serde value-serde]}]
  (consumed {:key-serde key-serde :value-serde value-serde}))

(defn topic->produced
  "Builds a Kafka Streams `Produced` from a topic config's serdes, attaching a
  custom stream partitioner when `:partition-fn` is present."
  [{:keys [key-serde value-serde partition-fn]}]
  (produced {:key-serde key-serde :value-serde value-serde :partition-fn partition-fn}))

(defn topic->repartitioned
  "Builds a Kafka Streams `Repartitioned` from a topic config's serdes, applying
  `:topic-name` and a `:partition-fn` partitioner when present."
  [{:keys [topic-name key-serde value-serde partition-fn]}]
  (repartitioned {:name topic-name :key-serde key-serde :value-serde value-serde
                  :partition-fn partition-fn}))

(defn topic->grouped
  "Builds a Kafka Streams `Grouped` from a topic config's `:key-serde` and
  `:value-serde`."
  [{:keys [key-serde value-serde]}]
  (grouped {:key-serde key-serde :value-serde value-serde}))

(defn topic->materialized
  "Builds a Kafka Streams `Materialized` store named by `:topic-name`, applying
  the topic config's key/value serdes when present."
  [{:keys [topic-name key-serde value-serde]}]
  (materialized {:store-name topic-name :key-serde key-serde :value-serde value-serde}))

(defn suppress-config->suppressed
  "Builds a Kafka Streams `Suppressed` from a suppression config, choosing the
  buffer bound from `:max-records`, `:max-bytes`, or `:until-time-limit-ms`."
  [{:keys [max-records max-bytes until-time-limit-ms]}]
  (let [config (cond
                 (not (nil? max-records))
                 (Suppressed$BufferConfig/maxRecords max-records)

                 (not (nil? max-bytes))
                 (Suppressed$BufferConfig/maxBytes max-bytes)

                 :else (Suppressed$BufferConfig/unbounded))]
    (if-some [time-limit until-time-limit-ms]
      (Suppressed/untilTimeLimit (Duration/ofMillis time-limit) config)
      (-> (.shutDownWhenFull ^Suppressed$BufferConfig config)
          Suppressed/untilWindowCloses))))

(declare clj-kstream clj-ktable clj-kgroupedtable clj-kgroupedstream
         clj-global-ktable clj-session-windowed-kstream
         clj-time-windowed-kstream)

(def ^:private kstream-memo
  "Returns a kstream for the topic, creating a new one if needed."
  (memoize
   (fn [streams-builder {:keys [topic-name] :as topic-config}]
     (clj-kstream
      (.stream ^StreamsBuilder streams-builder
               ^String topic-name
               ^Consumed (topic->consumed topic-config))))))

(def ^:private kstream-memo-patterned
  "Returns a kstream for the topic, creating a new one if needed."
  (memoize
   (fn [streams-builder topic-config topic-pattern]
     (clj-kstream
      (.stream ^StreamsBuilder streams-builder
               ^Pattern topic-pattern
               ^Consumed (topic->consumed topic-config))))))

(def ^:private ktable-memo
  "Returns a ktable for the topic, creating a new one if needed."
  (memoize
   (fn [streams-builder {:keys [topic-name] :as topic-config}
        store-name]
     (clj-ktable
      (.table ^StreamsBuilder streams-builder
              ^String topic-name
              ^Consumed (topic->consumed topic-config)
              ^Materialized (topic->materialized (assoc topic-config
                                                        :topic-name store-name)))))))

(deftype CljStreamsBuilder [^StreamsBuilder streams-builder]
  IStreamsBuilder

  (kstream
    [_ topic-config]
    (kstream-memo streams-builder topic-config))

  (kstream
    [_ topic-config topic-pattern]
    (kstream-memo-patterned streams-builder topic-config topic-pattern))

  (kstreams
    [_ topic-configs]
    (clj-kstream
     (let [topic-names (clojure.core/map :topic-name topic-configs)]
       (.stream streams-builder
                ^Collection topic-names
                ;; Assume all the topics use the same serdes.
                ^Consumed (topic->consumed (first topic-configs))))))

  (ktable
    [_ {:keys [topic-name] :as topic-config}]
    (ktable-memo streams-builder topic-config topic-name))

  (ktable
    [_ topic-config store-name]
    (ktable-memo streams-builder topic-config store-name))

  (global-ktable [_ {:keys [topic-name] :as topic-config}]
    (clj-global-ktable
     (.globalTable ^StreamsBuilder streams-builder
                   ^String topic-name
                   ^Consumed (topic->consumed topic-config))))

  (with-kv-state-store
    [builder {:keys [store-name key-serde value-serde] :as store-config}]
    (.addStateStore ^StreamsBuilder streams-builder
                    (Stores/keyValueStoreBuilder
                     (Stores/persistentKeyValueStore store-name)
                     key-serde
                     value-serde))
    builder)
  
  (streams-builder*
    [_]
    streams-builder))

(defn streams-builder
  "Makes a streams builder."
  []
  (CljStreamsBuilder. (StreamsBuilder.)))

(deftype CljKStream [^KStream kstream]
  IKStreamBase
  (join
    [_ ktable value-joiner-fn]
    (clj-kstream
     (.join ^KStream kstream
            ^KTable (ktable* ktable)
            ^ValueJoiner (value-joiner value-joiner-fn))))

  (left-join
    [_ ktable value-joiner-fn]
    (clj-kstream
     (.leftJoin ^KStream kstream
                ^KTable (ktable* ktable)
                ^ValueJoiner (value-joiner value-joiner-fn))))

  (left-join
    [_ ktable value-joiner-fn
     {key-serde :key-serde this-value-serde :value-serde}
     {other-value-serde :value-serde}]
    (clj-kstream
     (.leftJoin kstream
                ^KTable (ktable* ktable)
                ^ValueJoiner (value-joiner value-joiner-fn)
                (Joined/with key-serde this-value-serde other-value-serde))))

  (peek
    [_ peek-fn]
    (clj-kstream
     (.peek kstream ^ForeachAction (foreach-action peek-fn))))

  (filter
    [_ predicate-fn]
    (clj-kstream
     (.filter kstream ^Predicate (predicate predicate-fn))))

  (filter-not
    [_ predicate-fn]
    (clj-kstream
     (.filterNot kstream ^Predicate (predicate predicate-fn))))

  (group-by
    [_ key-value-mapper-fn]
    (clj-kgroupedstream
     (.groupBy kstream ^KeyValueMapper (select-key-value-mapper key-value-mapper-fn))))

  (group-by
    [_ key-value-mapper-fn topic-config]
    (clj-kgroupedstream
     (.groupBy kstream
               ^KeyValueMapper (select-key-value-mapper key-value-mapper-fn)
               ^Grouped (topic->grouped topic-config))))

  (map-values
    [_ value-mapper-fn]
    (clj-kstream
     (.mapValues kstream ^ValueMapper (value-mapper value-mapper-fn))))

  IKStream
  (branch
    [_ predicate-fns]
    ;; Kafka 4.0 removed KStream.branch(Predicate...). Reproduce its
    ;; positional-vector result via split()/BranchedKStream, collecting each
    ;; branch stream (in predicate order) through a Branched consumer.
    (let [collected (volatile! [])
          ^BranchedKStream split
          (clojure.core/reduce
           (fn [^BranchedKStream bks pred-fn]
             (.branch bks
                      ^Predicate (predicate pred-fn)
                      (Branched/withConsumer
                       (reify Consumer
                         (accept [_ ks] (vswap! collected conj ks))))))
           (.split ^KStream kstream)
           predicate-fns)]
      (.noDefaultBranch split)
      (mapv clj-kstream @collected)))

  (flat-map
    [_ key-value-mapper-fn]
    (clj-kstream
     (.flatMap kstream ^KeyValueMapper (key-value-flatmapper key-value-mapper-fn))))

  (for-each!
    [_ foreach-fn]
    (.foreach kstream ^ForeachAction (foreach-action foreach-fn))
    nil)

  (print!
    [_]
    (.print kstream (Printed/toSysOut))
    nil)

  (through
    [_ topic-config]
    ;; Kafka 4.0 removed KStream.through. Reimplement via repartition (its
    ;; official replacement): the stream continues downstream through an
    ;; internally-managed repartition topic rather than the named user topic.
    (clj-kstream
     (.repartition kstream ^Repartitioned (topic->repartitioned topic-config))))

  (to!
    [_ {:keys [topic-name] :as topic-config}]
    (.to kstream ^String topic-name ^Produced (topic->produced topic-config))
    nil)

  (flat-map-values
    [_ value-mapper-fn]
    (clj-kstream
     (.flatMapValues kstream ^ValueMapper (value-mapper value-mapper-fn))))

  (group-by-key
    [_]
    (clj-kgroupedstream
     (.groupByKey kstream)))

  (group-by-key
    [_ topic-config]
    (clj-kgroupedstream
     (.groupByKey ^KStream kstream
                  ^Grouped (topic->grouped topic-config))))

  (join-windowed
    [_ other-kstream value-joiner-fn windows]
    (clj-kstream
     (.join ^KStream kstream
            ^KStream (kstream* other-kstream)
            ^ValueJoiner (value-joiner value-joiner-fn)
            ^JoinWindows windows)))

  (join-windowed
    [_ other-kstream value-joiner-fn windows
     {key-serde :key-serde this-value-serde :value-serde}
     {other-value-serde :value-serde}]
    (clj-kstream
     (.join kstream
            ^KStream (kstream* other-kstream)
            ^ValueJoiner (value-joiner value-joiner-fn)
            ^JoinWindows windows
            (StreamJoined/with key-serde this-value-serde other-value-serde))))

  (left-join-windowed
    [_ other-kstream value-joiner-fn windows]
    (clj-kstream
     (.leftJoin ^KStream kstream
                ^KStream (kstream* other-kstream)
                ^ValueJoiner (value-joiner value-joiner-fn)
                ^JoinWindows windows)))

  (left-join-windowed
    [_ other-kstream value-joiner-fn windows
     {:keys [key-serde value-serde]}
     {other-value-serde :value-serde}]
    (clj-kstream
     (.leftJoin kstream
                ^KStream (kstream* other-kstream)
                ^ValueJoiner (value-joiner value-joiner-fn)
                ^JoinWindows windows
                (StreamJoined/with key-serde value-serde other-value-serde))))

  (map
    [_ key-value-mapper-fn]
    (clj-kstream
     (.map kstream ^KeyValueMapper (key-value-mapper key-value-mapper-fn))))

  (merge
    [_ other-kstream]
    (clj-kstream
      (.merge kstream
              ^KStream (kstream* other-kstream))))

  (outer-join-windowed
    [_ other-kstream value-joiner-fn windows]
    (clj-kstream
     (.outerJoin ^KStream kstream
                 ^KStream (kstream* other-kstream)
                 ^ValueJoiner (value-joiner value-joiner-fn)
                 ^JoinWindows windows)))

  (outer-join-windowed
    [_ other-kstream value-joiner-fn windows
     {key-serde :key-serde value-serde :value-serde}
     {other-value-serde :value-serde}]
    (clj-kstream
     (.outerJoin ^KStream kstream
                 ^KStream (kstream* other-kstream)
                 ^ValueJoiner (value-joiner value-joiner-fn)
                 ^JoinWindows windows
                 (StreamJoined/with key-serde value-serde other-value-serde))))

  (process!
    [_ processor-supplier-fn state-store-names]
    (.process ^KStream kstream
              ^ProcessorSupplier (processor-supplier processor-supplier-fn)
              ^"[Ljava.lang.String;" (into-array String state-store-names)))

  (select-key
    [_ select-key-value-mapper-fn]
    (clj-kstream
     (.selectKey ^KStream kstream
                 ^KeyValueMapper (select-key-value-mapper select-key-value-mapper-fn))))

  (transform
    [this transformer-supplier-fn]
    (transform this transformer-supplier-fn []))

  (transform
    [_ transformer-supplier-fn state-store-names]
    ;; Kafka 4.0 removed KStream.transform; route through the Processor API.
    (clj-kstream
     (.process ^KStream kstream
               (transformer-supplier->processor-supplier
                (transformer-supplier transformer-supplier-fn) false)
               ^"[Ljava.lang.String;" (into-array String state-store-names))))

  (flat-transform
    [this transformer-supplier-fn]
    (flat-transform this transformer-supplier-fn []))

  (flat-transform
    [_ transformer-supplier-fn state-store-names]
    (clj-kstream
     (.process ^KStream kstream
               (transformer-supplier->processor-supplier
                (transformer-supplier transformer-supplier-fn) true)
               ^"[Ljava.lang.String;" (into-array String state-store-names))))

  (transform-values
    [this value-transformer-supplier-fn]
    (transform-values this value-transformer-supplier-fn []))

  (transform-values
    [_ value-transformer-supplier-fn state-store-names]
    ;; Kafka 4.0 removed KStream.transformValues; route through processValues.
    (clj-kstream
     (.processValues ^KStream kstream
                     (value-transformer-supplier->fk-processor-supplier
                      (value-transformer-supplier value-transformer-supplier-fn) false)
                     ^"[Ljava.lang.String;" (into-array String state-store-names))))

  (flat-transform-values
    [this value-transformer-supplier-fn]
    (flat-transform-values this value-transformer-supplier-fn []))

  (flat-transform-values
    [_ value-transformer-supplier-fn state-store-names]
    (clj-kstream
     (.processValues ^KStream kstream
                     (value-transformer-supplier->fk-processor-supplier
                      (value-transformer-supplier value-transformer-supplier-fn) true)
                     ^"[Ljava.lang.String;" (into-array String state-store-names))))

  (join-global
    [_ global-ktable key-value-mapper-fn joiner-fn]
    (clj-kstream
     (.join kstream
            ^GlobalKTable (global-ktable* global-ktable)
            ^KeyValueMapper (select-key-value-mapper key-value-mapper-fn)
            ^ValueJoiner (value-joiner joiner-fn))))

  (left-join-global
    [_ global-ktable key-value-mapper-fn joiner-fn]
    (clj-kstream
     (.leftJoin kstream
                ^GlobalKTable (global-ktable* global-ktable)
                ^KeyValueMapper (select-key-value-mapper key-value-mapper-fn)
                ^ValueJoiner (value-joiner joiner-fn))))

  (kstream* [_]
    kstream))

(defn clj-kstream
  "Makes a CljKStream object."
  [kstream]
  (CljKStream. kstream))

(deftype CljKTable [^KTable ktable]
  IKStreamBase
  (join
    [_ other-ktable value-joiner-fn]
    (clj-ktable
     (.join ^KTable ktable
            ^KTable (ktable* other-ktable)
            ^ValueJoiner (value-joiner value-joiner-fn))))

  (join
    [_ other-ktable foreign-key-extractor-fn value-joiner-fn]
    (clj-ktable
      (.join ^KTable ktable
             ^KTable (ktable* other-ktable)
             ^Function (foreign-key-extractor foreign-key-extractor-fn)
             ^ValueJoiner (value-joiner value-joiner-fn))))

  (left-join
    [_ other-ktable value-joiner-fn]
    (clj-ktable
     (.leftJoin ^KTable ktable
                ^KTable (ktable* other-ktable)
                ^ValueJoiner (value-joiner value-joiner-fn))))

  (filter
    [_ predicate-fn]
    (clj-ktable
     (.filter ^KTable ktable
              ^Predicate (predicate predicate-fn))))

  (filter-not
    [_ predicate-fn]
    (clj-ktable
     (.filterNot ^KTable ktable
                 ^Predicate (predicate predicate-fn))))

  (map-values
    [_ value-mapper-fn]
    (clj-ktable
     (.mapValues ktable ^ValueMapper (value-mapper value-mapper-fn))))

  IKTable
  (group-by
    [_ key-value-mapper-fn]
    (clj-kgroupedtable
     (.groupBy ktable ^KeyValueMapper (key-value-mapper key-value-mapper-fn))))

  (group-by
    [_ key-value-mapper-fn topic-config]
    (clj-kgroupedtable
     (.groupBy ktable
               ^KeyValueMapper (key-value-mapper key-value-mapper-fn)
               ^Grouped (topic->grouped topic-config))))

  (outer-join
    [_ other-ktable value-joiner-fn]
    (clj-ktable
     (.outerJoin ^KTable ktable
                 ^KTable (ktable* other-ktable)
                 ^ValueJoiner (value-joiner value-joiner-fn))))

  (suppress
    [_ suppress-config]
    (clj-ktable
       (.suppress ^KTable ktable (suppress-config->suppressed suppress-config))))

  (to-kstream
    [_]
    (clj-kstream
     (.toStream ^KTable ktable)))

  (to-kstream
    [_ key-value-mapper-fn]
    (clj-kstream
     (.toStream ^KTable ktable
                ^KeyValueMapper (key-value-mapper key-value-mapper-fn))))

  (ktable* [_]
    ktable))

(defn clj-ktable
  "Makes a CljKTable object."
  [ktable]
  (CljKTable. ktable))

(deftype CljGlobalKTable [^GlobalKTable global-ktable]
  IGlobalKTable

  (global-ktable* [_]
    global-ktable))

(defn clj-global-ktable
  "Makes a CljKTable object."
  [global-ktable]
  (CljGlobalKTable. global-ktable))

(deftype CljKGroupedTable [^KGroupedTable kgroupedtable]
  IKGroupedBase
  (aggregate
    [_ initializer-fn adder-fn subtractor-fn topic]
    (clj-ktable
     (.aggregate ^KGroupedTable kgroupedtable
                 ^Initializer (initializer initializer-fn)
                 ^Aggregator (aggregator adder-fn)
                 ^Aggregator (aggregator subtractor-fn)
                 ^Materialized (topic->materialized topic))))

  (aggregate
    [_ initializer-fn adder-fn subtractor-fn]
    (clj-ktable
     (.aggregate ^KGroupedTable kgroupedtable
                 ^Initializer (initializer initializer-fn)
                 ^Aggregator (aggregator adder-fn)
                 ^Aggregator (aggregator subtractor-fn))))

  (count
    [_]
    (clj-ktable
     (.count ^KGroupedTable kgroupedtable)))

  (count
    [_ topic-config]
    (clj-ktable
     (.count ^KGroupedTable kgroupedtable
             ^Materialized (topic->materialized topic-config))))

  (reduce
    [_ adder-fn subtractor-fn topic-config]
    (clj-ktable
     (.reduce ^KGroupedTable kgroupedtable
              ^Reducer (reducer adder-fn)
              ^Reducer (reducer subtractor-fn)
              ^Materialized (topic->materialized topic-config))))

  (reduce
    [_ adder-fn subtractor-fn]
    (clj-ktable
     (.reduce ^KGroupedTable kgroupedtable
              ^Reducer (reducer adder-fn)
              ^Reducer (reducer subtractor-fn))))

  IKGroupedTable
  (kgroupedtable*
    [_]
    kgroupedtable))

(defn clj-kgroupedtable
  "Makes a CljKGroupedTable object."
  [kgroupedtable]
  (CljKGroupedTable. kgroupedtable))

(deftype CljKGroupedStream [^KGroupedStream kgroupedstream]
  IKGroupedBase
  (aggregate
    [_ initializer-fn aggregator-fn topic]
    (clj-ktable
     (.aggregate ^KGroupedStream kgroupedstream
                 ^Initializer (initializer initializer-fn)
                 ^Aggregator (aggregator aggregator-fn)
                 ^Materialized (topic->materialized topic))))

  (aggregate
    [_ initializer-fn aggregator-fn]
    (clj-ktable
     (.aggregate ^KGroupedStream kgroupedstream
                 ^Initializer (initializer initializer-fn)
                 ^Aggregator (aggregator aggregator-fn))))

  (count
    [_]
    (clj-ktable
     (.count ^KGroupedStream kgroupedstream)))

  (count
    [_ topic-config]
    (clj-ktable
     (.count ^KGroupedStream kgroupedstream
             ^Materialized (topic->materialized topic-config))))

  (reduce
    [_ reducer-fn topic-config]
    (clj-ktable
     (.reduce ^KGroupedStream kgroupedstream
              ^Reducer (reducer reducer-fn)
              ^Materialized (topic->materialized topic-config))))

  (reduce
    [_ reducer-fn]
    (clj-ktable
     (.reduce ^KGroupedStream kgroupedstream
              ^Reducer (reducer reducer-fn))))

  IKGroupedStream
  (windowed-by-time
    [_ windows]
    (clj-time-windowed-kstream
     (.windowedBy ^KGroupedStream kgroupedstream ^Windows windows)))

  (windowed-by-session
    [_ windows]
    (clj-session-windowed-kstream
     (.windowedBy ^KGroupedStream kgroupedstream ^SessionWindows windows)))

  (kgroupedstream*
    [_]
    kgroupedstream))

(defn clj-kgroupedstream
  "Makes a CljKGroupedStream object."
  [kgroupedstream]
  (CljKGroupedStream. kgroupedstream))

(deftype CljTimeWindowedKStream [^TimeWindowedKStream windowed-kstream]
  IKGroupedBase
  (aggregate
    [_ initializer-fn aggregator-fn topic]
    (clj-ktable
     (.aggregate ^TimeWindowedKStream windowed-kstream
                 ^Initializer (initializer initializer-fn)
                 ^Aggregator (aggregator aggregator-fn)
                 ^Materialized (topic->materialized topic))))

  (count
    [_]
    (clj-ktable
     (.count ^TimeWindowedKStream windowed-kstream)))

  (count
    [_ topic-config]
    (clj-ktable
     (.count ^TimeWindowedKStream windowed-kstream
             ^Materialized (topic->materialized topic-config))))

  (reduce
    [_ reducer-fn topic-config]
    (clj-ktable
     (.reduce ^TimeWindowedKStream windowed-kstream
              ^Reducer (reducer reducer-fn)
              ^Materialized (topic->materialized topic-config))))

  ITimeWindowedKStream
  (time-windowed-kstream*
    [_]
    windowed-kstream))

(defn clj-time-windowed-kstream
  "Makes a CljTimeWindowedKStream object."
  [windowed-kstream]
  (CljTimeWindowedKStream. windowed-kstream))

(deftype CljSessionWindowedKStream [^SessionWindowedKStream windowed-kstream]
  IKGroupedBase
  (aggregate
    [_ initializer-fn aggregator-fn merger-fn topic]
    (clj-ktable
     (.aggregate ^SessionWindowedKStream windowed-kstream
                 ^Initializer (initializer initializer-fn)
                 ^Aggregator (aggregator aggregator-fn)
                 ^Merger (merger merger-fn)
                 ^Materialized (topic->materialized topic))))

  (count
    [_]
    (clj-ktable
     (.count ^SessionWindowedKStream windowed-kstream)))

  (count
    [_ topic-config]
    (clj-ktable
     (.count ^SessionWindowedKStream windowed-kstream
             ^Materialized (topic->materialized topic-config))))

  (reduce
    [_ reducer-fn topic-config]
    (clj-ktable
     (.reduce ^SessionWindowedKStream windowed-kstream
              ^Reducer (reducer reducer-fn)
              ^Materialized (topic->materialized topic-config))))

  ISessionWindowedKStream
  (session-windowed-kstream*
    [_]
    windowed-kstream))

(defn clj-session-windowed-kstream
  "Makes a CljSessionWindowedKStream object."
  [windowed-kstream]
  (CljSessionWindowedKStream. windowed-kstream))
