(ns jackdaw.schema-registry-test
  (:require [clojure.test :refer [deftest is testing]]
            [jackdaw.schema-registry :as registry]
            [jackdaw.serdes.avro.schema-registry :as avro-registry])
  (:import [io.confluent.kafka.schemaregistry.avro AvroSchema]
           [io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient]
           [io.confluent.kafka.schemaregistry.client.rest.entities Config]))

(set! *warn-on-reflection* true)

(def schema
  (AvroSchema. "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}"))

(deftest client-construction
  (is (instance? CachedSchemaRegistryClient
               (registry/client "http://schema-registry.invalid" 16))))

(deftest schema-registry-management
  (let [client (avro-registry/mock-client)]
    (testing "subjects, registration, versions, and compatibility"
      (is (= [] (registry/list-subjects client)))
      (is (= 1 (registry/register-schema! client "users-value" schema)))
      (is (= ["users-value"] (registry/list-subjects client)))
      (is (= 1 (registry/schema-version client "users-value" schema)))
      (is (true? (registry/compatible? client "users-value" schema)))
      (is (= [1] (registry/schema-versions client "users-value"))))
    (testing "configuration get/set"
      (is (= "BACKWARD" (:compatibility-level
                          (registry/set-config! client "users-value" "BACKWARD"))))
      (is (= "BACKWARD" (:compatibility-level
                          (registry/get-config client "users-value")))))
    (testing "subject and version deletion"
      (is (= 1 (registry/delete-schema-version! client "users-value" 1)))
      (is (= [] (registry/delete-subject! client "users-value")))
      (is (= [] (registry/list-subjects client))))))

(deftest config-object-is-accepted
  (let [client (avro-registry/mock-client)
        config (Config. "FULL")]
    (is (= "FULL" (:compatibility-level
                    (registry/set-config! client "" config))))))
