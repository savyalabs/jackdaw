(ns jackdaw.admin-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [jackdaw.admin :as admin]
   [jackdaw.data :as data]
   [manifold.deferred :as d])
  (:import
   (org.apache.kafka.common Node KafkaFuture)
   (org.apache.kafka.clients.admin AlterConfigOp AlterConfigOp$OpType
                                  MockAdminClient)))

(set! *warn-on-reflection* false)

(extend MockAdminClient
  admin/Client
  (-> admin/client-impl
              (merge {:alter-topics* (fn [_this topics]
                               (d/future [:altered topics]))
              :describe-configs* (fn [_this configs]
                                   (d/future
                                     (into {} (map #(vector % {"some-key" "some-value"}) configs))))
              :create-partitions* (fn [_this partitions] (d/future partitions))
              :describe-consumer-groups* (fn [_this group-ids] (d/future group-ids))
              :list-consumer-group-offsets* (fn [_this group-offsets] (d/future group-offsets))
              :describe-acls* (fn [_this filter] (d/future filter))
              :create-acls* (fn [_this acls] (d/future acls))
              :delete-acls* (fn [_this filters] (d/future filters))
              :describe-client-quotas* (fn [_this filter] (d/future filter))
              :alter-client-quotas* (fn [_this alterations] (d/future alterations))
              :describe-log-dirs* (fn [_this broker-ids] (d/future broker-ids))
              :alter-partition-reassignments* (fn [_this reassignments] (d/future reassignments))
              :list-partition-reassignments* (fn [_this partitions] (d/future partitions))
              :describe-metadata-quorum* (fn [_this] (d/future :quorum))})))

(defn set= [a b]
  (= (set a)
     (set b)))

(def test-topics
  {:foo {:topic-name "foo"
         :partition-count 15
         :replication-factor 3
         :topic-config {}}

   :bar {:topic-name "bar"
         :partition-count 15
         :replication-factor 3
         :topic-config {}}})

(defn node-seq [id host]
  (lazy-seq
   (cons (Node. id (str host "-" id) 1234)
         (node-seq (inc id) host))))

(defn kfuture []
  (KafkaFuture.))

(def test-cluster (take 3 (node-seq 0 "test-host")))

(defn with-mock-admin-client [cluster f]
  (let [client (MockAdminClient. cluster (first cluster))]
    (f client)))

(deftest test-new-topic
  (doseq [[_k info] test-topics]
    (let [t (data/map->NewTopic info)
          msg (format "cannot create topic from %s" t)]
      (is (instance? org.apache.kafka.clients.admin.NewTopic t) msg))))

(deftest test-create-topics!
  (with-mock-admin-client test-cluster
    (fn [client]
      (admin/create-topics! client (vals test-topics))
      (is (set= (map :topic-name (vals test-topics))
                (keys (admin/describe-topics client)))))))

(deftest test-list-topics
  (with-mock-admin-client test-cluster
    (fn [client]
      (admin/create-topics! client (vals test-topics))
      (is (set= (map #(select-keys % [:topic-name]) (vals test-topics))
                (admin/list-topics client))))))

(deftest test-topic-exists?
  (with-mock-admin-client test-cluster
    (fn [client]
      (admin/create-topics! client (vals test-topics))
      (doseq [[_k info] test-topics]
        (is (admin/topic-exists? client info))))))

(deftest test-retry-exists?
  (with-mock-admin-client test-cluster
    (fn [client]
      (testing "returns true if the topic is created within specified time"
        (let [foo (:foo test-topics)
              p (d/future
                  (admin/retry-exists? client foo 3 30))]
          (Thread/sleep 50)
          (admin/create-topics! client [foo])
          (is (= true @p))))

      (testing "returns false if the topic is not created within specified time"
        (let [bar (:bar test-topics)
              p (d/future
                  (admin/retry-exists? client bar 3 30))]
          (Thread/sleep 100)
          (admin/create-topics! client [bar])
          (is (= false @p)))))))

(deftest test-describe-topics
  (with-mock-admin-client test-cluster
    (fn [client]
      (admin/create-topics! client (vals test-topics))
      (doseq [[_topic-name topic-info] (admin/describe-topics client)]
        (is (set= [:is-internal? :partition-info]
                  (keys topic-info)))))))

(deftest test-describe-cluster
  (with-mock-admin-client test-cluster
    (fn [client]
      (admin/create-topics! client (vals test-topics))
      (is (set= [:cluster-id :controller :nodes]
                (keys (admin/describe-cluster client)))))))

(deftest test-topics-ready?
  (with-mock-admin-client test-cluster
    (fn [client]
      (admin/create-topics! client (vals test-topics))
      (is (admin/topics-ready? client (vals test-topics))))))

(deftest test-partition-ids-of-topics
  (with-mock-admin-client test-cluster
    (fn [client]
      (admin/create-topics! client (vals test-topics))
      (doseq [[t info] test-topics]
        (is (= (:partition-count info)
               (-> (admin/partition-ids-of-topics client)
                   (get (name t))
                   count)))))))

(deftest test-delete-topics!
  (with-mock-admin-client test-cluster
    (fn [client]
      (let [{:keys [foo bar]} test-topics]
        (admin/create-topics! client [foo bar])
        (admin/delete-topics! client [foo])
        (is (= [{:topic-name "bar"}]
               (admin/list-topics client)))))))

(deftest test-alter-topic-config!
  (with-mock-admin-client test-cluster
    (fn [client]
      (let [{:keys [foo bar]} test-topics]
        (admin/create-topics! client [foo bar])
        (is (= :altered
               (-> (admin/alter-topic-config!
                    client (map #(update % :replication-factor inc)
                                [foo bar]))
                   first)))))))

(deftest topics->configs-builds-set-operations
  (let [configs (#'admin/topics->configs
                 [{:topic-name "foo"
                   :topic-config {"cleanup.policy" "compact"
                                  "retention.ms" "60000"}}])
        [resource operations] (first configs)]
    (is (= "foo" (.name resource)))
    (is (instance? java.util.Collection operations))
    (is (every? #(instance? AlterConfigOp %) operations))
    (is (= #{["cleanup.policy" "compact" AlterConfigOp$OpType/SET]
             ["retention.ms" "60000" AlterConfigOp$OpType/SET]}
           (set (map (fn [^AlterConfigOp operation]
                       (let [entry (.configEntry operation)]
                         [(.name entry) (.value entry) (.opType operation)]))
                     operations))))))

(deftest test-broker-config
  (with-mock-admin-client test-cluster
    (fn [client]
      (let [{:keys [foo bar]} test-topics]
        (admin/create-topics! client [foo bar])
        (is (= {"some-key" "some-value"}
               (admin/get-broker-config client 0)))))))

(deftest test-describe-topics-config
  (with-mock-admin-client test-cluster
    (fn [client]
      (let [{:keys [foo bar]} test-topics]
        (admin/create-topics! client [foo bar])
        (let [description (admin/describe-topics-configs client [foo bar])]
          (is (set= ["foo" "bar"]
                    (map :name (keys description))))
          (doseq [cfg (vals description)]
            (is (set= {"some-key" "some-value"} cfg))))))))

(deftest test-operational-admin-client-operations
  (is (every? #(contains? admin/client-impl %)
              [:create-partitions*
               :describe-consumer-groups*
               :list-consumer-group-offsets*
               :describe-acls*
               :create-acls*
               :delete-acls*
               :describe-client-quotas*
               :alter-client-quotas*
               :describe-log-dirs*
               :alter-partition-reassignments*
               :list-partition-reassignments*
               :describe-metadata-quorum*])))

(deftest test-operational-admin-functions
  (with-mock-admin-client test-cluster
    (fn [client]
      (let [partitions {"foo" :new-partitions}
            group-ids ["group-a"]
            offsets {"group-a" :offset-spec}
            filter :acl-filter
            acls [:acl]
            filters [:acl-filter]
            alterations [:quota-alteration]
            broker-ids [0 1]
            reassignments {:partition :reassignment}
            partitions-to-list [:partition-a]]
        (is (= partitions (admin/create-partitions! client partitions)))
        (is (= group-ids (admin/describe-consumer-groups client group-ids)))
        (is (= offsets (admin/list-consumer-group-offsets client offsets)))
        (is (= filter (admin/describe-acls client filter)))
        (is (= acls (admin/create-acls! client acls)))
        (is (= filters (admin/delete-acls! client filters)))
        (is (= filter (admin/describe-client-quotas client filter)))
        (is (= alterations (admin/alter-client-quotas! client alterations)))
        (is (= broker-ids (admin/describe-log-dirs client broker-ids)))
        (is (= reassignments
               (admin/alter-partition-reassignments! client reassignments)))
        (is (= partitions-to-list
               (admin/list-partition-reassignments client partitions-to-list)))
        (is (= :quorum (admin/describe-metadata-quorum client)))))))
