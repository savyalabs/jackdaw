(ns jackdaw.schema-registry
  "Management operations for a Confluent Schema Registry.

  Functions in this namespace operate on a
  `io.confluent.kafka.schemaregistry.client.SchemaRegistryClient`, so callers
  can use either a real HTTP client or the Confluent mock client in tests.")

(import '[io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient
         SchemaRegistryClient]
        '[io.confluent.kafka.schemaregistry.client.rest.entities Config])

(set! *warn-on-reflection* true)

(defn client
  "Create a cached HTTP Schema Registry client."
  ^SchemaRegistryClient [^String url max-capacity]
  {:pre [(string? url) (pos-int? max-capacity)]}
  (CachedSchemaRegistryClient. url ^int max-capacity))

(defn client?
  "Return true when x is a Schema Registry client."
  [x]
  (instance? SchemaRegistryClient x))

(defn list-subjects
  "Return all registered subjects in deterministic order."
  [^SchemaRegistryClient client]
  {:pre [(client? client)]}
  (->> (.getAllSubjects client) sort vec))

(defn register-schema!
  "Register a ParsedSchema under subject and return its integer ID."
  [^SchemaRegistryClient client ^String subject schema]
  {:pre [(client? client) (string? subject)
         (instance? io.confluent.kafka.schemaregistry.ParsedSchema schema)]}
  (.register client subject ^io.confluent.kafka.schemaregistry.ParsedSchema schema))

(defn schema-version
  "Return the version of schema registered under subject."
  [^SchemaRegistryClient client ^String subject schema]
  {:pre [(client? client) (string? subject)
         (instance? io.confluent.kafka.schemaregistry.ParsedSchema schema)]}
  (.getVersion client subject ^io.confluent.kafka.schemaregistry.ParsedSchema schema))

(defn compatible?
  "Return whether schema is compatible with the subject's latest version."
  [^SchemaRegistryClient client ^String subject schema]
  {:pre [(client? client) (string? subject)
         (instance? io.confluent.kafka.schemaregistry.ParsedSchema schema)]}
  (.testCompatibility client subject ^io.confluent.kafka.schemaregistry.ParsedSchema schema))

(defn schema-versions
  "Return all registered versions for subject."
  [^SchemaRegistryClient client ^String subject]
  {:pre [(client? client) (string? subject)]}
  (vec (.getAllVersions client subject)))

(defn- config->map
  [^Config config]
  (cond-> {:compatibility-level (.getCompatibilityLevel config)}
    (.getCompatibilityPolicy config) (assoc :compatibility-policy (.getCompatibilityPolicy config))
    (.getCompatibilityGroup config) (assoc :compatibility-group (.getCompatibilityGroup config))
    (.getAlias config) (assoc :alias (.getAlias config))))

(defn- ->config
  ^Config [config]
  (cond
    (instance? Config config) config
    (string? config) (Config. ^String config)
    (map? config) (doto (Config.)
                    (.setCompatibilityLevel ^String (:compatibility-level config))
                    (.setCompatibilityPolicy ^String (:compatibility-policy config))
                    (.setCompatibilityGroup ^String (:compatibility-group config)))
    :else (throw (IllegalArgumentException.
                  "config must be a compatibility string, map, or Config"))))

(defn get-config
  "Return subject configuration as a Clojure map.

  Pass the empty subject to read the global configuration."
  [^SchemaRegistryClient client ^String subject]
  {:pre [(client? client) (string? subject)]}
  (config->map (.getConfig client subject)))

(defn set-config!
  "Set subject configuration and return the resulting configuration map."
  [^SchemaRegistryClient client ^String subject config]
  {:pre [(client? client) (string? subject)]}
  (config->map (.updateConfig client subject (->config config))))

(defn delete-schema-version!
  "Delete one schema version and return the deleted version."
  ([client subject version]
   (delete-schema-version! client subject version false))
  ([^SchemaRegistryClient client ^String subject version permanent?]
   {:pre [(client? client) (string? subject) (integer? version)
          (boolean? permanent?)]}
   (.deleteSchemaVersion client subject (str version) (boolean permanent?))))

(defn delete-subject!
  "Delete a subject and return its deleted version numbers."
  ([client subject]
   (delete-subject! client subject false))
  ([^SchemaRegistryClient client ^String subject permanent?]
   {:pre [(client? client) (string? subject) (boolean? permanent?)]}
   (vec (.deleteSubject client subject (boolean permanent?)))))
