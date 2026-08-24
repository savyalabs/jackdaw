(ns jackdaw.streams
  "Kafka streams protocols."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:refer-clojure :exclude [count map merge reduce group-by filter peek])
  (:require [clojure.string :as str]
            [jackdaw.streams.interop :as interop]
            [jackdaw.streams.protocols :as p])
  (:import org.apache.kafka.streams.KafkaStreams
           org.apache.kafka.streams.StreamsBuilder
           org.apache.kafka.streams.KafkaStreams$State
           org.apache.kafka.streams.KafkaStreams$StateListener
           org.apache.kafka.streams.Topology
           [org.apache.kafka.streams
            KeyQueryMetadata StreamsMetadata StoreQueryParameters KeyValue]
           [org.apache.kafka.streams.state
            HostInfo KeyValueIterator QueryableStoreTypes ReadOnlyKeyValueStore]
           org.apache.kafka.common.serialization.Serializer
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.streams.errors
            StreamsUncaughtExceptionHandler
            StreamsUncaughtExceptionHandler$StreamThreadExceptionResponse]
           [org.apache.kafka.streams.processor StateRestoreListener]))

(set! *warn-on-reflection* true)

;; StreamsBuilder

(defn kstream
  "Creates a KStream that will consume messages from the specified topic."
  ([streams-builder topic-config]
   {:pre [(map? topic-config)]}
   (p/kstream streams-builder topic-config))
  ([streams-builder topic-config topic-pattern]
   {:pre [(map? topic-config)]}
   (p/kstream streams-builder topic-config topic-pattern)))

(defn kstreams
  "Creates a KStream that will consume messages from the specified topics."
  [streams-builder topic-configs]
  (p/kstreams streams-builder topic-configs))

(defn ktable
  "Creates a KTable that will consist of data from the specified topic."
  ([streams-builder topic-config]
   (p/ktable streams-builder topic-config))
  ([streams-builder topic-config store-name]
   (p/ktable streams-builder topic-config store-name)))

(defn global-ktable
  "Creates a GlobalKTable that will consist of data from the specified
  topic."
  [streams-builder topic-config]
  (p/global-ktable streams-builder topic-config))

(defn source-topics
  "Gets the names of source topics for the topology."
  [streams-builder]
  (p/source-topics streams-builder))

(defn with-kv-state-store
  "Adds a persistent state store to the topology with the configured name
  and serdes"
  [streams-builder store-config]
  (p/with-kv-state-store streams-builder store-config))

(defn streams-builder*
  "Returns the underlying KStreamBuilder."
  [streams-builder]
  (p/streams-builder* streams-builder))

;; IKStreamBase

(defn join
  "Combines the values of the KStream-or-KTable and the KTable that
  share the same key or a foreign key using an inner join."
  ([kstream-or-ktable ktable value-joiner-fn]
   (p/join kstream-or-ktable ktable value-joiner-fn))
  ([kstream-or-ktable ktable foreign-key-extractor-fn value-joiner-fn]
   (p/join kstream-or-ktable ktable foreign-key-extractor-fn value-joiner-fn)))

(defn left-join
  "Creates a KStream from the result of calling `value-joiner-fn` with
  each element in the KStream and the value in the KTable with the same
  key."
  ([kstream ktable value-joiner-fn]
   (p/left-join kstream ktable value-joiner-fn))
  ([kstream ktable value-joiner-fn this-topic-config other-topic-config]
   (p/left-join kstream ktable value-joiner-fn this-topic-config other-topic-config)))

(defn filter
  "Creates a KStream that consists of all elements that satisfy a predicate."
  [kstream predicate-fn]
  (p/filter kstream predicate-fn))

(defn filter-not
  "Creates a KStream that consists of all elements that do not satisfy a
  predicate."
  [kstream predicate-fn]
  (p/filter-not kstream predicate-fn))

(defn group-by
  "Groups the records of this KStream/KTable using the key-value-mapper-fn."
  ([ktable key-value-mapper-fn]
   (p/group-by ktable key-value-mapper-fn))
  ([ktable key-value-mapper-fn topic-config]
   (p/group-by ktable key-value-mapper-fn topic-config)))

(defn peek
  "Performs the action defined by `peek-fn` on each element of the input
  KStream, returning that stream untransformed."
  [kstream peek-fn]
  (p/peek kstream peek-fn))

(defn map-values
  "Creates a KStream that is the result of calling `value-mapper-fn` on each
  element of the input stream."
  [kstream value-mapper-fn]
  (p/map-values kstream value-mapper-fn))

(defn print!
  "Prints the elements of the stream to *out*."
  [kstream]
  (p/print! kstream))

(defn through
  "Materializes a stream to a topic, and returns a new KStream that will
  consume messages from the topic."
  [kstream topic-config]
  (p/through kstream topic-config))

(defn to
  "Materializes a stream to a topic."
  [kstream topic-config]
  (p/to! kstream topic-config))

;; IKStream

(defn branch
  "Returns a list of KStreams, one for each of the `predicate-fns`
  provided."
  [kstream predicate-fns]
  (p/branch kstream predicate-fns))

(defn flat-map
  "Creates a KStream that will consist of the concatenation of messages
  returned by calling `key-value-mapper-fn` on each key/value pair in the
  input stream."
  [kstream key-value-mapper-fn]
  (p/flat-map kstream key-value-mapper-fn))

(defn flat-map-values
  "Creates a KStream that will consist of the concatenation of the values
  returned by calling `value-mapper-fn` on each value in the input stream."
  [kstream value-mapper-fn]
  (p/flat-map-values kstream value-mapper-fn))

(defn for-each!
  "Performs an action on each element of KStream."
  [kstream foreach-fn]
  (p/for-each! kstream foreach-fn))

(defn group-by-key
  "Groups records with the same key into a KGroupedStream."
  ([kstream]
   (p/group-by-key kstream))
  ([kstream topic-config]
   (p/group-by-key kstream topic-config)))

(defn join-windowed
  "Combines the values of two streams that share the same key using a
  windowed inner join."
  ([kstream other-kstream value-joiner-fn windows]
   (p/join-windowed kstream other-kstream value-joiner-fn windows))
  ([kstream other-kstream value-joiner-fn windows this-topic-config other-topic-config]
   (p/join-windowed kstream other-kstream value-joiner-fn windows this-topic-config other-topic-config)))

(defn left-join-windowed
  "Combines the values of two streams that share the same key using a
  windowed left join."
  ([kstream other-kstream value-joiner-fn windows]
   (p/left-join-windowed kstream other-kstream value-joiner-fn windows))
  ([kstream other-kstream value-joiner-fn windows this-topic-config other-topic-config]
   (p/left-join-windowed kstream other-kstream value-joiner-fn windows this-topic-config other-topic-config)))

(defn map
  "Creates a KStream that consists of the result of applying
  `key-value-mapper-fn` to each key/value pair in the input stream."
  [kstream key-value-mapper-fn]
  (p/map kstream key-value-mapper-fn))

(defn outer-join-windowed
  "Combines the values of two streams that share the same key using a
  windowed outer join."
  ([kstream other-kstream value-joiner-fn windows]
   (p/outer-join-windowed kstream other-kstream value-joiner-fn windows))
  ([kstream other-kstream value-joiner-fn windows this-topic-config other-topic-config]
   (p/outer-join-windowed kstream other-kstream value-joiner-fn windows this-topic-config other-topic-config)))

(defn process!
  "Applies `processor-fn` to each item in the input stream."
  [kstream processor-fn state-store-names]
  (p/process! kstream processor-fn state-store-names))

(defn select-key
  "Create a new key from the current key and value.

   `select-key-value-mapper-fn` should be a function that takes a key-value
   pair, and returns the value of the new key. Here is example multiplies each
   key by 10:

   ```(fn [[k v]] (* 10 k))```"
  [kstream select-key-value-mapper-fn]
  (p/select-key kstream select-key-value-mapper-fn))

(defn transform
  "Creates a KStream that consists of the results of applying the transformer
  to each key/value in the input stream."
  ([kstream transformer-supplier-fn]
   (p/transform kstream transformer-supplier-fn))
  ([kstream transformer-supplier-fn state-store-names]
   (p/transform kstream transformer-supplier-fn state-store-names)))

(defn flat-transform
  "Creates a KStream that consists of the results of applying the transformer
  to each value in the input stream. Result of the transform should be iterable,
  and the resulting stream is as per flatMap"
  ([kstream transformer-supplier-fn]
   (p/flat-transform kstream transformer-supplier-fn))
  ([kstream transformer-supplier-fn state-store-names]
   (p/flat-transform kstream transformer-supplier-fn state-store-names)))

(defn transform-values
  "Creates a KStream that consists of the results of applying the transformer
  to each value in the input stream."
  ([kstream value-transformer-supplier-fn]
   (p/transform-values kstream value-transformer-supplier-fn))
  ([kstream value-transformer-supplier-fn state-store-names]
   (p/transform-values kstream value-transformer-supplier-fn state-store-names)))

(defn flat-transform-values
  "Creates a KStream that consists of the results of applying the transformer
  to each value in the input stream. Result of the transform should be iterable,
  and the resulting stream is as per flatMap"
  ([kstream value-transformer-supplier-fn]
   (p/flat-transform-values kstream value-transformer-supplier-fn))
  ([kstream value-transformer-supplier-fn state-store-names]
   (p/flat-transform-values kstream value-transformer-supplier-fn state-store-names)))

(defn join-global
  "Inner-joins each record of `kstream` to a record in `global-ktable`, looking
  up the table by the key returned from `(kv-mapper k v)` and combining the
  values with `joiner`. Records with no matching table entry are dropped."
  [kstream global-ktable kv-mapper joiner]
  (p/join-global kstream global-ktable kv-mapper joiner))

(defn left-join-global
  "Like `join-global`, but keeps each `kstream` record even when `global-ktable`
  has no entry for `(kv-mapper k v)`, calling `joiner` with a nil table value."
  [kstream global-ktable kv-mapper joiner]
  (p/left-join-global kstream global-ktable kv-mapper joiner))

(defn merge
  "Merges `kstream` and `other` into a single KStream containing all of their
  records."
  [kstream other]
  (p/merge kstream other))

(defn kstream*
  "Returns the underlying KStream object."
  [kstream]
  (p/kstream* kstream))

;; IKTable

(defn outer-join
  "Combines the values of two KTables that share the same key using an outer
  join."
  [ktable other-ktable value-joiner-fn]
  (p/outer-join ktable other-ktable value-joiner-fn))

(defn to-kstream
  "Converts a KTable to a KStream."
  ([ktable]
   (p/to-kstream ktable))
  ([ktable key-value-mapper-fn]
   (p/to-kstream ktable key-value-mapper-fn)))

(defn suppress
  "Suppress some updates from this changelog stream"
  [ktable suppressed]
  (p/suppress ktable suppressed))

(defn ktable*
  "Returns the underlying KTable object."
  [ktable]
  (p/ktable* ktable))

;; IKGroupedBase

(defn aggregate
  "Aggregates values by key into a new KTable."
  ([kgrouped initializer-fn adder-fn]
   (p/aggregate kgrouped initializer-fn adder-fn))
  ([kgrouped initializer-fn aggregator-fn subtractor-fn-or-topic-config]
   (p/aggregate kgrouped initializer-fn aggregator-fn subtractor-fn-or-topic-config))
  ([kgrouped initializer-fn adder-fn subtractor-or-merger-fn topic-config]
   (p/aggregate kgrouped initializer-fn adder-fn subtractor-or-merger-fn topic-config)))

(defn count
  "Counts the number of records by key into a new KTable."
  ([kgrouped]
   (p/count kgrouped))
  ([kgrouped name]
   (p/count kgrouped name)))

(defn reduce
  "Combines values of a stream by key into a new KTable."
  ([kgrouped adder-fn subtractor-fn topic-config]
   (p/reduce kgrouped adder-fn subtractor-fn topic-config))
  ([kgrouped reducer-fn subtractor-fn-or-topic-config]
   (p/reduce kgrouped reducer-fn subtractor-fn-or-topic-config))
  ([kgrouped reducer-fn]
   (p/reduce kgrouped reducer-fn)))

;; IKGroupedTable

(defn kgroupedtable*
  "Returns the underlying KGroupedTable object."
  [kgroupedtable]
  (p/kgroupedtable* kgroupedtable))

;; IKGroupedStream

(defn window-by-time
  "Windows the KStream"
  ([kgroupedstream window]
   (p/windowed-by-time kgroupedstream window)))

(defn window-by-session
  "Windows the KStream"
  ([kgroupedstream window]
   (p/windowed-by-session kgroupedstream window)))

(defn kgroupedstream*
  "Returns the underlying KGroupedStream object."
  ([kgroupedstream]
   (p/kgroupedstream* kgroupedstream)))

;; IGlobalKTable

(defn global-ktable*
  "Returns the underlying GlobalKTable"
  [globalktable]
  (p/global-ktable* globalktable))

(defn streams-builder
  "Returns a new, empty streams-builder, the entry point for defining a topology."
  []
  (interop/streams-builder))

(defn kafka-streams
  "Makes a Kafka Streams object."
  ([builder opts]
   (let [props (java.util.Properties.)]
     (.putAll props opts)
     (KafkaStreams. ^Topology (.build ^StreamsBuilder (streams-builder* builder))
                    ^java.util.Properties props))))

(defn start
  "Starts processing."
  [kafka-streams]
  (.start ^KafkaStreams kafka-streams))

(defn close
  "Stops the kafka streams."
  [kafka-streams]
  (.close ^KafkaStreams kafka-streams))

(defn state->keyword
  "Converts a KafkaStreams$State enum value to a lower-cased, dash-separated
  keyword (e.g. RUNNING -> :running, NOT_RUNNING -> :not-running)."
  [^KafkaStreams$State state]
  (-> state .name str/lower-case (str/replace #"_" "-") keyword))

(defn state
  "Returns the current lifecycle state of `k-streams` as a keyword (see
  `state->keyword`)."
  [^KafkaStreams k-streams]
  (-> k-streams .state state->keyword))

;; Interactive queries

(defn- host-info->data
  [^HostInfo host-info]
  (when host-info
    {:host (.host host-info)
     :port (.port host-info)}))

(defn- streams-metadata->data
  [^StreamsMetadata metadata]
  {:host-info (host-info->data (.hostInfo metadata))
   :host (.host metadata)
   :port (.port metadata)
   :state-store-names (set (.stateStoreNames metadata))
   :topic-partitions (set (.topicPartitions metadata))
   :standby-topic-partitions (set (.standbyTopicPartitions metadata))
   :standby-state-store-names (set (.standbyStateStoreNames metadata))})

(defn- key-query-metadata->data
  [^KeyQueryMetadata metadata]
  {:active-host (host-info->data (.activeHost metadata))
   :standby-hosts (set (map host-info->data (.standbyHosts metadata)))
   :partition (.partition metadata)})

(defn metadata-for-all-streams-clients
  "Returns metadata for every Kafka Streams instance in the application.

  Each metadata entry is a map containing host/port and the stores and
  partitions assigned to that instance."
  [^KafkaStreams k-streams]
  (mapv streams-metadata->data (.metadataForAllStreamsClients k-streams)))

(defn streams-metadata-for-store
  "Returns metadata for the Kafka Streams instances hosting `store-name`."
  [^KafkaStreams k-streams ^String store-name]
  (mapv streams-metadata->data (.streamsMetadataForStore k-streams store-name)))

(defn query-metadata-for-key
  "Returns the active and standby hosts for `key` in `store-name`.

  `serializer` is used by Kafka Streams to determine the key's partition."
  [^KafkaStreams k-streams ^String store-name key ^Serializer serializer]
  (key-query-metadata->data
   (.queryMetadataForKey k-streams store-name key serializer)))

(defn store
  "Returns a local read-only key/value store.

  Optional options are `:partition` to query one partition and
  `:stale-stores?` to allow querying stale stores."
  ([^KafkaStreams k-streams ^String store-name]
   (store k-streams store-name {}))
  ([^KafkaStreams k-streams ^String store-name
    {:keys [partition stale-stores?]}]
   (let [params (StoreQueryParameters/fromNameAndType
                 store-name
                 (QueryableStoreTypes/keyValueStore))
         ^StoreQueryParameters params (cond-> params
                                        (some? partition)
                                        (.withPartition (int partition))
                                        stale-stores?
                                        (.enableStaleStores))]
     (.store k-streams params))))

(defn store-get
  "Returns the value for `key` in a read-only key/value `store`."
  [^ReadOnlyKeyValueStore store key]
  (.get store key))

(defn- store-iterator->vec
  [^KeyValueIterator iterator]
  (try
    (loop [entries []]
      (if (.hasNext iterator)
        (let [^KeyValue entry (.next iterator)]
          (recur (conj entries [(.key entry) (.value entry)])))
        entries))
    (finally
      (.close iterator))))

(defn store-range
  "Returns `[key value]` pairs in the inclusive range `from-key` to `to-key`."
  [^ReadOnlyKeyValueStore store from-key to-key]
  (store-iterator->vec (.range store from-key to-key)))

(defn store-all
  "Returns all `[key value]` pairs in a read-only key/value `store`."
  [^ReadOnlyKeyValueStore store]
  (store-iterator->vec (.all store)))

(defn store-approximate-num-entries
  "Returns the approximate number of entries in a read-only key/value `store`."
  [^ReadOnlyKeyValueStore store]
  (.approximateNumEntries store))

(defn set-state-listener
  "Registers `f` as a Kafka Streams state listener and returns `k-streams`.

  `f` is called with the new state and old state whenever the lifecycle state
  changes."
  [^KafkaStreams k-streams f]
  (.setStateListener k-streams
                     (reify KafkaStreams$StateListener
                       (^void onChange [_
                                        ^KafkaStreams$State new-state
                                        ^KafkaStreams$State old-state]
                         (f new-state old-state))))
  k-streams)

(defn set-uncaught-exception-handler
  "Registers `f` as the uncaught stream-thread exception handler.

  `f` receives the thrown `Throwable` and must return a
  `StreamThreadExceptionResponse`."
  [^KafkaStreams k-streams f]
  (.setUncaughtExceptionHandler
   k-streams
   (reify StreamsUncaughtExceptionHandler
     (^StreamsUncaughtExceptionHandler$StreamThreadExceptionResponse
      handle [_ ^Throwable exception]
       (f exception))))
  k-streams)

(defn set-global-state-restore-listener
  "Registers callbacks for global state-store restoration and returns
  `k-streams`.

  `callbacks` may contain `:on-restore-start`, `:on-batch-restored`, and
  `:on-restore-end` functions. Each receives the arguments supplied by Kafka's
  `StateRestoreListener` method with the corresponding name."
  [^KafkaStreams k-streams {:keys [on-restore-start on-batch-restored on-restore-end]}]
  (let [on-restore-start (or on-restore-start (fn [& _] nil))
        on-batch-restored (or on-batch-restored (fn [& _] nil))
        on-restore-end (or on-restore-end (fn [& _] nil))]
    (.setGlobalStateRestoreListener
     k-streams
     (reify StateRestoreListener
       (^void onRestoreStart [_
                              ^TopicPartition topic-partition
                              ^String store-name
                              ^long starting-offset
                              ^long ending-offset]
         (on-restore-start topic-partition store-name starting-offset ending-offset))
       (^void onBatchRestored [_
                               ^TopicPartition topic-partition
                               ^String store-name
                               ^long batch-end-offset
                               ^long num-restored]
         (on-batch-restored topic-partition store-name batch-end-offset num-restored))
       (^void onRestoreEnd [_
                            ^TopicPartition topic-partition
                            ^String store-name
                            ^long total-restored]
         (on-restore-end topic-partition store-name total-restored)))))
  k-streams)

(defn metrics
  "Returns the Kafka Streams metrics map."
  [^KafkaStreams k-streams]
  (.metrics k-streams))

(defn local-threads-metadata
  "Returns metadata for the locally running Kafka Streams threads."
  [^KafkaStreams k-streams]
  (.metadataForLocalThreads k-streams))
