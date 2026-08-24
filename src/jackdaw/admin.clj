(ns jackdaw.admin
  "Tools to administer or interact with a Kafka cluster.

  Wraps the `AdminClient` API, replacing the Scala admin APIs.

  Like the underlying `AdminClient` API, this namespace is subject to
  change and should be considered of alpha stability."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require
   [jackdaw.data :as jd]
   [manifold.deferred :as d])
  (:import [java.util Collection Optional Properties]
           [org.apache.kafka.clients.admin AdminClient AlterConfigOp
            AlterConfigOp$OpType AlterConfigsResult
            DescribeTopicsOptions DescribeClusterOptions DescribeConfigsOptions
            ListPartitionReassignmentsOptions ListPartitionReassignmentsResult]))

(set! *warn-on-reflection* true)

(defprotocol Client
  (alter-topics* [this topics])
  (create-topics* [this topics])
  (delete-topics* [this topics])
  (describe-topics* [this topics])
  (describe-configs* [this configs])
  (describe-cluster* [this])
  (list-topics* [this])
  (create-partitions* [this partitions])
  (describe-consumer-groups* [this group-ids])
  (list-consumer-group-offsets* [this group-offsets])
  (describe-acls* [this filter])
  (create-acls* [this acls])
  (delete-acls* [this filters])
  (describe-client-quotas* [this filter])
  (alter-client-quotas* [this alterations])
  (describe-log-dirs* [this broker-ids])
  (alter-partition-reassignments* [this reassignments])
  (list-partition-reassignments* [this partitions])
  (describe-metadata-quorum* [this]))

(def client-impl
  {:alter-topics* (fn [this topics]
                    (d/future
                      ;; Kafka 4.0 removed AdminClient.alterConfigs; use the
                      ;; incremental variant (callers pass a map of
                      ;; ConfigResource -> Collection<AlterConfigOp>).
                      @(.all ^AlterConfigsResult
                             (.incrementalAlterConfigs ^AdminClient this topics))))
   :create-topics* (fn [this topics]
                    (d/future
                      @(.all (.createTopics ^AdminClient this ^Collection topics))))
   :delete-topics*  (fn [this topics]
                      (d/future
                        @(.all (.deleteTopics ^AdminClient this ^Collection topics))))
   :describe-topics* (fn [this topics]
                       (d/future
                         ;; Kafka 4.0 removed DescribeTopicsResult.all; use allTopicNames.
                         @(.allTopicNames (.describeTopics ^AdminClient this ^Collection topics (DescribeTopicsOptions.)))))
   :describe-configs* (fn [this configs]
                        (d/future
                          @(.all (.describeConfigs ^AdminClient this configs (DescribeConfigsOptions.)))))
   :describe-cluster* (fn [this]
                        (d/future
                          (jd/datafy (.describeCluster ^AdminClient this (DescribeClusterOptions.)))))
   :list-topics* (fn [this]
                   (d/future
                     @(.names (.listTopics ^AdminClient this))))
   :create-partitions* (fn [this partitions]
                         (d/future
                           @(.all (.createPartitions ^AdminClient this ^java.util.Map partitions))))
   :describe-consumer-groups* (fn [this group-ids]
                                (d/future
                                  @(.all (.describeConsumerGroups ^AdminClient this ^Collection group-ids))))
   :list-consumer-group-offsets* (fn [this group-offsets]
                                   (d/future
                                     @(.all (.listConsumerGroupOffsets ^AdminClient this ^java.util.Map group-offsets))))
   :describe-acls* (fn [this filter]
                     (d/future
                       @(.values (.describeAcls ^AdminClient this filter))))
   :create-acls* (fn [this acls]
                   (d/future
                     @(.all (.createAcls ^AdminClient this ^Collection acls))))
   :delete-acls* (fn [this filters]
                   (d/future
                     @(.all (.deleteAcls ^AdminClient this ^Collection filters))))
   :describe-client-quotas* (fn [this filter]
                              (d/future
                                @(.entities (.describeClientQuotas ^AdminClient this filter))))
   :alter-client-quotas* (fn [this alterations]
                           (d/future
                             @(.all (.alterClientQuotas ^AdminClient this ^Collection alterations))))
   :describe-log-dirs* (fn [this broker-ids]
                         (d/future
                           @(.allDescriptions (.describeLogDirs ^AdminClient this ^Collection broker-ids))))
   :alter-partition-reassignments* (fn [this reassignments]
                                     (d/future
                                       @(.all (.alterPartitionReassignments ^AdminClient this ^java.util.Map reassignments))))
   :list-partition-reassignments* (fn [this partitions]
                                    (d/future
                                      @(.reassignments
                                        ^ListPartitionReassignmentsResult
                                        (.listPartitionReassignments
                                         ^AdminClient this
                                         ^Optional (if (nil? partitions)
                                                     (Optional/empty)
                                                     (Optional/of ^java.util.Set (set partitions)))
                                         (ListPartitionReassignmentsOptions.)))))
   :describe-metadata-quorum* (fn [this]
                                (d/future
                                  @(.quorumInfo (.describeMetadataQuorum ^AdminClient this))))})

(extend AdminClient
  Client
  client-impl)

(defn ->AdminClient
  "Given a Kafka properties map having `\"bootstrap.servers\"`, return
  an `AdminClient` bootstrapped off of the configured servers."
  ^AdminClient [kafka-config]
  {:pre [(get kafka-config "bootstrap.servers")]}
  (AdminClient/create ^Properties (jd/map->Properties kafka-config)))

(defn client?
  "Predicate.

  Return `true` if and only if given an `AdminClient` instance."
  [x]
  (instance? AdminClient x))

(defn list-topics
  "Given an `AdminClient`, return a seq of topic records, being the
  topics on the cluster."
  [^AdminClient client]
  {:pre [(client? client)]}
  (->> @(list-topics* client)
       ;; Let the caller decide whether to sort the result.
       sort
       (map #(hash-map :topic-name %))))

(defn topic-exists?
  "Verifies the existence of the topic.

  Does not verify any config. details or values."
  [^AdminClient client {:keys [topic-name] :as topic}]
  {:pre [(client? client)
         (string? topic-name)]}
  (contains? (set (list-topics client)) {:topic-name topic-name}))

(defn retry-exists?
  "Returns `true` if topic exists. Otherwise spins as configured."
  [client topic num-retries wait-ms]
  (cond (topic-exists? client topic)
        true

        (zero? num-retries)
        false

        :else
        (do (Thread/sleep ^long wait-ms)
            (recur client topic (dec num-retries) wait-ms))))

(defn create-topics!
  "Given an `AdminClient` and a collection of topic descriptors,
  create the specified topics with their configuration(s).

  Does not block until the created topics are ready. It may take some
  time for replicas and leaders to be chosen for newly created
  topics.

  See `#'topics-ready?`, `#'topic-exists?` and `#'retry-exists?` for
  tools with which to wait for topics to be ready."
  [^AdminClient client topics]
  {:pre [(client? client)
         (sequential? topics)]}
  @(create-topics* client (map jd/map->NewTopic topics)))

(defn describe-topics
  "Given an `AdminClient` and an optional collection of topic
  descriptors, return a map from topic names to topic
  descriptions.

  If no topics are provided, describes all topics.

  Note that the topic description does NOT include the topic's
  configuration.See `#'describe-topic-config` for that capability."
  ([^AdminClient client]
   {:pre [(client? client)]}
   (describe-topics client (list-topics client)))
  ([^AdminClient client topics]
   {:pre [(client? client)
          (sequential? topics)]}
   (->> @(describe-topics* client (map :topic-name topics))
        (map (fn [[k v]] [k (jd/datafy v)]))
        (into {}))))

(defn describe-topics-configs
  "Given an `AdminClient` and a collection of topic descriptors, returns
  the selected topics' live configuration as a map from topic names to
  configured properties to metadata about each property including its
  current value."
  [^AdminClient client topics]
  {:pre [(client? client)
         (sequential? topics)]}
  (->> @(describe-configs* client (map #(-> % :topic-name jd/->topic-resource) topics))
       (into {})
       (reduce-kv (fn [m k v]
                    (assoc m (jd/datafy k) (jd/datafy v)))
                  {})))

(defn topics-ready?
  "Given an `AdminClient` and a sequence topic descriptors, return
  `true` if and only if all listed topics have a leader and in-sync
  replicas.

  This can be used to determine if some set of newly created topics
  are healthy yet, or detect whether leader re-election has finished
  following the demise of a Kafka broker."
  [^AdminClient client topics]
  {:pre [(client? client)
         (sequential? topics)]}
  (->> @(describe-topics* client (map :topic-name topics))
       (every? (fn [[_topic-name {:keys [partition-info]}]]
                 (every? (fn [part-info]
                           (and (boolean (:leader part-info))
                                (seq (:isr part-info))))
                         partition-info)))))

(defn- topics->configs
  ^java.util.Map [topics]
  (into {}
        (map (fn [{:keys [topic-name topic-config] :as t}]
               {:pre [(string? topic-name)
                      (map? topic-config)]}
               [(jd/->ConfigResource jd/+topic-config-resource-type+
                                     topic-name)
                (map (fn [[key value]]
                       (AlterConfigOp.
                        (jd/->ConfigEntry key value)
                        AlterConfigOp$OpType/SET))
                     topic-config)]))
        topics))

(defn alter-topic-config!
  "Given an `AdminClient` and a sequence of topic descriptors having
  `:topic-config`, alters the live configuration of the specified
  topics to correspond to the specified `:topic-config`."
  [^AdminClient client topics]
  {:pre [(client? client)
         (sequential? topics)]}
  @(alter-topics* client (topics->configs topics)))

(defn delete-topics!
  "Given an `AdminClient` and a sequence of topic descriptors, marks the
  topics for deletion.

  Does not block until the topics are deleted, just until the deletion
  request(s) are acknowledged."
  [^AdminClient client topics]
  {:pre [(client? client)
         (sequential? topics)]}
  @(delete-topics* client (map :topic-name topics)))

(defn partition-ids-of-topics
  "Given an `AdminClient` and an optional sequence of topics, produces a
  mapping from topic names to a sequence of the partition IDs for that
  topic.

  By default, enumerates the partition IDs for all topics."
  ([^AdminClient client]
   {:pre [(client? client)]}
   (partition-ids-of-topics client (list-topics client)))
  ([^AdminClient client topics]
   {:pre [(client? client)
          (sequential? topics)]}
   (->> (describe-topics client topics)
        (map (fn [[topic-name {:keys [partition-info]}]]
               [topic-name (mapv :partition partition-info)]))
        (into {}))))

(defn describe-cluster
  "Returns a `DescribeClusterResult` describing the cluster."
  [^AdminClient client]
  {:pre [(client? client)]}
  (-> @(describe-cluster* client)
      jd/datafy))

(defn get-broker-config
  "Returns the broker config as a map.

  Broker-id is an int, typically 0-2, get the list of valid broker ids
  using describe-cluster"
  [^AdminClient client broker-id]
  {:pre [(client? client)]}
  (-> @(describe-configs* client [(jd/->broker-resource (str broker-id))])
      vals first jd/datafy))

(defn create-partitions!
  "Increase the partition count for topics using Kafka `NewPartitions` values."
  [^AdminClient client partitions]
  {:pre [(client? client)
         (map? partitions)]}
  @(create-partitions* client partitions))

(defn describe-consumer-groups
  "Describe the named consumer groups."
  [^AdminClient client group-ids]
  {:pre [(client? client)
         (sequential? group-ids)]}
  @(describe-consumer-groups* client group-ids))

(defn list-consumer-group-offsets
  "List offsets for consumer groups using Kafka offset request specs."
  [^AdminClient client group-offsets]
  {:pre [(client? client)
         (map? group-offsets)]}
  @(list-consumer-group-offsets* client group-offsets))

(defn describe-acls
  "Describe ACL bindings matching a Kafka ACL filter."
  [^AdminClient client filter]
  {:pre [(client? client)]}
  @(describe-acls* client filter))

(defn create-acls!
  "Create Kafka ACL bindings."
  [^AdminClient client acls]
  {:pre [(client? client)
         (sequential? acls)]}
  @(create-acls* client acls))

(defn delete-acls!
  "Delete Kafka ACL bindings matching the supplied filters."
  [^AdminClient client filters]
  {:pre [(client? client)
         (sequential? filters)]}
  @(delete-acls* client filters))

(defn describe-client-quotas
  "Describe client quotas matching a Kafka quota filter."
  [^AdminClient client filter]
  {:pre [(client? client)]}
  @(describe-client-quotas* client filter))

(defn alter-client-quotas!
  "Alter Kafka client quotas using `ClientQuotaAlteration` values."
  [^AdminClient client alterations]
  {:pre [(client? client)
         (sequential? alterations)]}
  @(alter-client-quotas* client alterations))

(defn describe-log-dirs
  "Describe log directories for the supplied broker IDs."
  [^AdminClient client broker-ids]
  {:pre [(client? client)
         (sequential? broker-ids)]}
  @(describe-log-dirs* client broker-ids))

(defn alter-partition-reassignments!
  "Alter partition reassignments using a TopicPartition-to-Optional map."
  [^AdminClient client reassignments]
  {:pre [(client? client)
         (map? reassignments)]}
  @(alter-partition-reassignments* client reassignments))

(defn list-partition-reassignments
  "List partition reassignments, optionally restricted to partitions."
  ([^AdminClient client]
   (list-partition-reassignments client nil))
  ([^AdminClient client partitions]
   {:pre [(client? client)
          (or (nil? partitions) (sequential? partitions))]}
   @(list-partition-reassignments* client partitions)))

(defn describe-metadata-quorum
  "Describe the KRaft metadata quorum."
  [^AdminClient client]
  {:pre [(client? client)]}
  @(describe-metadata-quorum* client))
