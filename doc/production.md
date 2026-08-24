# Production patterns

This guide collects deployment-oriented examples that complement the [Client](client.md),
[Streams](streams.md), and [AdminClient](admin.md) guides. The snippets are
runnable-shaped, but they intentionally do not connect to Kafka by themselves. Replace
the placeholder endpoints and environment variables in an application that has access to
your cluster.

The examples target the Kafka 4.3.1 artifacts resolved by this repository. Methods that
are not currently wrapped by Jackdaw use verified Java interop against those artifacts;
they should not be read as APIs already provided by Jackdaw.

## Transactional produce, then commit

Jackdaw's `jc/produce!` sends a `ProducerRecord` and returns a dereferenceable result.
The transaction lifecycle is the Kafka `Producer` API, so use Java interop for
`initTransactions`, `beginTransaction`, and `commitTransaction`. A unique
`transactional.id` is required for each live producer instance (and must be stable for
that instance across restarts according to the application's fencing strategy).

```clojure
(ns production.transactional-producer
  (:require [jackdaw.client :as jc])
  (:import [org.apache.kafka.clients.producer KafkaProducer]))

(def producer-config
  {"bootstrap.servers" "kafka.example.invalid:9093"
   "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"
   "acks" "all"
   "enable.idempotence" "true"
   "transactional.id" "orders-writer-INSTANCE-ID"})

(defn produce-then-commit!
  [records]
  (with-open [producer (jc/producer producer-config)]
    ;; KafkaProducer.initTransactions must complete before beginTransaction.
    (.initTransactions ^KafkaProducer producer)
    (.beginTransaction ^KafkaProducer producer)
    (try
      (doseq [{:keys [key value]} records]
        @(jc/produce! producer {:topic-name "orders"} key value))
      (.commitTransaction ^KafkaProducer producer)
      :committed
      (catch Throwable t
        ;; Abort before rethrowing so the caller can retry or alert.
        (.abortTransaction ^KafkaProducer producer)
        (throw t)))))
```

For consume-transform-produce workflows, use `sendOffsetsToTransaction` with the
consumer's `ConsumerGroupMetadata` before committing the transaction. That is a
separate Kafka API operation and is not exposed as a Jackdaw convenience function.

## SASL/SSL client configuration

All Jackdaw client constructors accept a string-keyed configuration map. The same shape
can be passed to `jc/producer`, `jc/consumer`, and `ja/->AdminClient`. Keep secrets out
of source control; the example reads them from the process environment and uses a
truststore path supplied by deployment configuration.

```clojure
(ns production.sasl-ssl
  (:require [jackdaw.client :as jc]))

(defn required-env [name]
  (or (System/getenv name)
      (throw (ex-info (str "missing environment variable: " name)
                      {:variable name}))))

(def client-config
  {"bootstrap.servers" "kafka.example.invalid:9093"
   "security.protocol" "SASL_SSL"
   "sasl.mechanism" "SCRAM-SHA-512"
   "sasl.jaas.config"
   (format (str "org.apache.kafka.common.security.scram.ScramLoginModule "
                "required username=\"%s\" password=\"%s\";")
           (required-env "KAFKA_USERNAME")
           (required-env "KAFKA_PASSWORD"))
   "ssl.truststore.location" (required-env "KAFKA_TRUSTSTORE_LOCATION")
   "ssl.truststore.password" (required-env "KAFKA_TRUSTSTORE_PASSWORD")
   "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"})

(with-open [producer (jc/producer client-config)]
  @(jc/produce! producer {:topic-name "orders"} "order-1" "created"))
```

Use the corresponding deserializer properties for consumers. If your platform provides
the trust chain through the JVM truststore, omit the explicit truststore properties and
configure that truststore outside the application.

## Interactive state-store queries

A state store must be materialized with a name before it can be queried. Jackdaw exposes
`with-kv-state-store` for adding a persistent key-value store, while the interactive
query methods below are Kafka Streams methods accessed through verified Java interop.
The query is local to the running `KafkaStreams` instance; a production HTTP/RPC layer
should route a key to the host returned by `queryMetadataForKey` when the active store is
remote.

```clojure
(ns production.state-query
  (:require [jackdaw.streams :as j])
  (:import [org.apache.kafka.common.serialization Serde]
           [org.apache.kafka.streams KafkaStreams StoreQueryParameters]
           [org.apache.kafka.streams.state QueryableStoreTypes ReadOnlyKeyValueStore]))

(defn local-order
  [^KafkaStreams streams order-id]
  (let [store-params (StoreQueryParameters/fromNameAndType
                       "orders-by-id"
                       (QueryableStoreTypes/keyValueStore))
        store (.store streams store-params)]
    (.get ^ReadOnlyKeyValueStore store order-id)))

(defn order-host
  [^KafkaStreams streams order-id ^Serde key-serde]
  ;; Use the returned HostInfo to route a remote query in the application API.
  (let [metadata (.queryMetadataForKey streams "orders-by-id" order-id key-serde)]
    (.activeHost metadata)))

;; `streams` is the app returned by (j/kafka-streams topology streams-config).
;; (local-order streams "order-1")
;; (order-host streams "order-1" key-serde)
```

The `StoreQueryParameters/fromNameAndType` and `KafkaStreams/store` calls query a
read-only view; do not mutate a state store from the query layer. The application must
also wait until the streams state is `RUNNING` before treating a local query as ready.

## Graceful `KafkaStreams` shutdown

Register the state listener before starting the application, install a shutdown hook,
and close with a bounded `Duration`. The listener is useful for readiness and lifecycle
metrics; `close(Duration)` is the operation that actually stops the client.

```clojure
(ns production.stream-lifecycle
  (:require [jackdaw.streams :as j])
  (:import [java.time Duration]
           [org.apache.kafka.streams KafkaStreams KafkaStreams$State KafkaStreams$StateListener]))

(defn install-lifecycle!
  [^KafkaStreams streams]
  (.setStateListener
   streams
   (reify KafkaStreams$StateListener
     (onChange [_ new-state old-state]
       (println "Kafka Streams state" (.name old-state) "->" (.name new-state))
       (when (= KafkaStreams$State/ERROR new-state)
         (binding [*out* *err*]
           (println "Kafka Streams entered ERROR"))))))
  (.addShutdownHook
   (Runtime/getRuntime)
   (Thread.
    (fn []
      ;; close(Duration) returns whether shutdown completed before the timeout.
      (when-not (.close streams (Duration/ofSeconds 30))
        (binding [*out* *err*]
          (println "Kafka Streams did not close within 30 seconds")))))))

;; Construct the topology and app as in doc/streams.md, then:
;; (install-lifecycle! app)
;; (j/start app)
```

Make the shutdown hook idempotent if your process can invoke application shutdown from
more than one signal path. Do not call `System/exit` from the state listener.

## Error handling and admin-operation propagation

Kafka 4.x uses `StreamsUncaughtExceptionHandler` for stream-thread failures. Its handler
must return a response; `SHUTDOWN_CLIENT` is a conservative default for an application
that cannot safely continue. Jackdaw's admin functions dereference their underlying
Kafka futures, so they throw on failure. Wrap the call in a Manifold deferred when the
caller needs asynchronous composition, and attach `manifold.deferred/catch` to preserve
one error path.

```clojure
(ns production.errors
  (:require [jackdaw.admin :as ja]
            [manifold.deferred :as d])
  (:import [org.apache.kafka.streams KafkaStreams]
           [org.apache.kafka.streams.errors
            StreamsUncaughtExceptionHandler
            StreamsUncaughtExceptionHandler$StreamThreadExceptionResponse]))

(defn install-error-handler!
  [^KafkaStreams streams report!]
  (.setUncaughtExceptionHandler
   streams
   (reify StreamsUncaughtExceptionHandler
     (handle [_ throwable]
       (report! throwable)
       StreamsUncaughtExceptionHandler$StreamThreadExceptionResponse/SHUTDOWN_CLIENT))))

(defn create-topic-deferred
  [admin-client topic]
  (-> (d/future (ja/create-topics! admin-client [topic]))
      (d/catch (fn [error]
                ;; This branch receives Kafka's cause, including authorization
                ;; and timeout failures. Return a value or rethrow as appropriate.
                (throw (ex-info "Kafka admin operation failed"
                                {:topic (:topic-name topic)}
                                error))))))
```

`d/future` starts the blocking Jackdaw admin call off-thread, and `d/catch` propagates
the failure through the returned deferred. Close the `AdminClient` in the application's
normal lifecycle path after all dependent deferred operations have completed.
