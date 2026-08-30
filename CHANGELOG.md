# Changelog

## [1.4.1] - 2026-08-30

### Fixed

- Correct the README's supported-versions note, which still referred to Jackdaw
  1.3.6.

## [1.4.0] - 2026-08-24

### Added

- Kafka Streams interactive state-store query wrappers for local key/value
  lookups, store metadata, and remote-instance routing.
- Expanded Kafka Admin operational APIs for partition increases,
  consumer-group descriptions and offsets, ACLs, quotas, log directories, and
  partition reassignment.
- Transactional producer helpers and consumer commit, pause, and rebalance
  listener wrappers.
- Kafka Streams DSL option builders for Named, Materialized, Grouped, Joined,
  StreamJoined, Repartitioned, Produced, and Consumed.
- Streams lifecycle and observability wrappers for state listeners, uncaught
  exception handlers, restore listeners, metrics, and thread metadata.
- Schema Registry management API for subject listing, registration, and
  compatibility checks.
- Failure-focused tests covering transactional, commit, rebalance, and
  reassignment error paths using Kafka's own Mock classes.
- Expanded production-configuration examples in the documentation.

## [1.3.9] - 2026-08-17

### Fixed

- `produce!` sends the caller's key in every keyed arity. It previously sent
  the topic map as the record key, so those messages were partitioned on the
  wrong key.
- `alter-topic-config!` builds `AlterConfigOp` entries for
  `incrementalAlterConfigs`, fixing topic config alteration on Kafka 4.x.

## [1.3.8] - 2026-08-01

### Changed
- Declare Aleph in the pom as an optional dependency so cljdoc can analyze the integration namespaces. It remains non-transitive for consumers.

## [1.3.7] - 2026-07-12

### Changed
- deps.edn-native build (tools.build; Leiningen via lein-tools-deps).

### Unreleased

#### Changed

- Make `deps.edn` and `tools.build` the canonical build, test, and release
  toolchain. The default Kaocha suite now runs broker-free unit tests; the full
  integration suite remains available for environments running the Kafka stack.

### [1.3.6] - [2026-07-07]

- Dependency currency: Kafka 4.3.0 -> 4.3.1, Confluent
  (schema-registry-client, avro/json-schema serializers) 8.2.1 -> 8.3.0,
  Jackson core/databind 2.21.2 -> 2.22.0, org.clojure/java.data 1.0.95 ->
  1.4.120, data.fressian 1.0.0 -> 1.1.1, tools.logging 1.2.4 -> 1.3.1,
  core.cache 1.0.225 -> 1.2.263. Validated against the live KRaft broker
  (115 tests / 837 assertions).
- Releases now publish to Clojars via a tag-triggered (`v*`) GitHub
  Actions workflow instead of manual `lein deploy`.

### [1.3.5] - [2026-06-16]

- Dependency currency: aleph 0.6.1 -> 0.9.9 (test transport), manifold
  0.4.0 -> 0.5.0, danlentz/clj-uuid 0.1.9 -> 0.2.5, metosin/jsonista
  0.3.7 -> 1.0.0. Jackson stays pinned at 2.21.2 (core + databind) so the
  Kafka/Confluent/Avro alignment is unaffected. Validated against the live
  KRaft broker (115 tests / 837 assertions).

### [1.3.4] - [2026-06-14]

- Standardize README structure and badges (docs only).

### [1.3.3] - [2026-06-11]

- Add docstrings to the 95 public functions/macros that were missing them
  across the client, serdes, streams, data, and test-machine namespaces. No
  behavior changes.

### [1.3.2] - [2026-06-08]

- Support recursively-defined Avro records (#328). A record that references
  itself (e.g. the Avro spec's `LongList` linked list) no longer overflows the
  stack while building its serde; the coercion build now registers a forward
  reference for the in-progress record so the self-reference resolves to it.

### [1.3.1] - [2026-06-08]

Runtime Kafka-4.x fixes that the mock (TopologyTestDriver) suite could not
catch; surfaced by the live-broker integration tests. Anyone on 1.3.0 using a
real consumer, the test-machine, or a streams uncaught-exception handler should
upgrade.

- Consumer assignment: a 0ms poll no longer completes the group rebalance under
  Kafka 4.x, so `seek-to-end`/`seek-to-beginning` and the test-machine's
  consumer would not position correctly. Added `client/poll-for-assignments`
  (poll until partitions are assigned) and use it in the eager seeks and the
  kafka transport.
- `setUncaughtExceptionHandler`: Kafka 4.0 removed the
  `Thread.UncaughtExceptionHandler` overload; the streams fixtures now reify
  `StreamsUncaughtExceptionHandler`.
- Test infra modernized to a single-node KRaft stack (no ZooKeeper); CI runs the
  full suite incl. live-broker integration tests on a JDK 17/21 matrix.

### [1.3.0] - [2026-06-08]

Maintenance fork published as `net.clojars.savya/jackdaw` (the upstream
`fundingcircle/jackdaw` is unmaintained). This release modernizes jackdaw for
Apache Kafka 4.x.

- **Apache Kafka 4.x / Confluent 8.x.** Bumped `org.apache.kafka`
  clients/streams/test-utils `3.3.2` → `4.3.0` and the `io.confluent` serdes
  `7.3.2` → `8.2.1`; Clojure `1.11.1` → `1.12.5`. Pinned `jackson-core`/
  `jackson-databind` to `2.21.2` to resolve a transitive conflict.
- **Streams DSL ported off Kafka-4.0 removals** (the public jackdaw API is
  unchanged):
  - `branch` now uses `split()`/`BranchedKStream` (still returns the positional
    vector of streams).
  - `through` is reimplemented via `repartition`. **Breaking:** it now routes
    through an internally-managed repartition topic rather than the named user
    topic.
  - `transform`/`flat-transform`/`transform-values`/`flat-transform-values` are
    reimplemented on the Processor API (`process`/`processValues`). **Breaking:**
    context-aware transformer sugar (`transformer-with-ctx`,
    `value-transformer-with-ctx`) now receives the new `api.ProcessorContext`;
    read record metadata via `recordMetadata()` rather than `.topic()` etc.
- **admin**: `alterConfigs` → `incrementalAlterConfigs`; `DescribeTopicsResult`
  uses `allTopicNames`.
- Cleared all reflection warnings (#322) including `admin/retry-exists?` (#369).
- `map->Properties` stringifies scalar config values so integer-valued config
  keys read back correctly (#311).
- Avro union-record coercion uses `Schema.Field.hasDefaultValue`, fixing union
  records whose fields are defaulted (#349, #258).
- `MockSchemaRegistryClient` registers the Avro + JSON providers (Confluent 8.x
  no longer registers them by default).

Requires **JDK 17+** and **Kafka 4.x** brokers/clients.

### [0.9.12] - [2023-12-05]
- Support for Foreign Key joins [#365](https://github.com/FundingCircle/jackdaw/pull/365) (Issue [#364])
- add manifold and keep aleph in dev dependencies [#360](https://github.com/FundingCircle/jackdaw/pull/360). Users of test-machine will have to add aleph to the test deps in their app.

### [0.9.11] - [2023-04-25]
- v0.9.10 introduced some reflection warnings. Whilst harmless seeing `.deleteTopics` appear in your logs at app startup is a little unsettling. [#358](https://github.com/FundingCircle/jackdaw/pull/358)
- fix to re-introduce correct handling of timestamped consumer records. Broke test asserting windowed results. [#356](https://github.com/FundingCircle/jackdaw/pull/356)

### [0.9.10] - [2023-04-03]

- Kafka 3.x series compatibility. Specifically 3.3.2 with this release (including native support for Arm macs) (https://github.com/FundingCircle/jackdaw/pull/353,  https://github.com/FundingCircle/jackdaw/pull/355)

### [0.9.9] - [2023-01-30]

- Fix JSON_SR serde to correctly "Clojurize" deeply nested maps

### [0.9.8] - [2022-10-18]

- Trivial release to fix release tagging issue

### [0.9.7] - [2022-10-17]

- Trivial release to fix API publishing in cljdocs
- Fix project.edn repo typo

### [0.9.6] - [2022-08-01]

- Add clj-kondo and fix all lint warnings and errors [#323](https://github.com/FundingCircle/jackdaw/pull/323)
- Allow Kafka Schema registry library to create a client rather than do it in Jackdaw (this allows security properties to be recognised) [#330](https://github.com/FundingCircle/jackdaw/pull/330)

### [0.9.5] - [2022-05-26]

* Move away from deprecated class ConsumerRecordFactory (to prepare migration to Kafka Streams 3.2.0)

### [0.9.4] - [2022-05-23]

* More verbose avro serialization exception errors

### [0.9.3] - [2021-11-24]

* Move libraries overrides back to dependencies.

### [0.9.2] - [2021-11-23]

* Fixed CVE-2021-37137, CVE-2021-37136 and CVE-2021-36090.

### [0.9.1] - [2021-11-19]

* Fixed Circle CI build process.

### [0.9.0] 

This version has been tagged in the repo but unreleased due to the broken build process.

* Drop clj-time dependency.
* Added confluent schema registry support for JSON Schema.
* Removed dependency on deprecated `org.apache.kafka.streams.kstream.Serialized` class.
* Added simple helpers in lambdas for using transformers more easily in a stream. [#305](https://github.com/FundingCircle/jackdaw/pull/305)
* Added the `flatTransform` and `flatTransfromValues` calls to the core streams interface. [#305](https://github.com/FundingCircle/jackdaw/pull/305)
* Aded a helper function to add KV state stores to a streams builder. [#305](https://github.com/FundingCircle/jackdaw/pull/305)
* Add KStream-KTable inner join.

## [0.8.0] - 2021-05-13
* Update Kafka to 2.8.0 (confluent 6.1.1) [#292](https://github.com/FundingCircle/jackdaw/pull/292)
* Improve test-machine documentation [#287](https://github.com/FundingCircle/jackdaw/pull/287)
* Fix CI pipeline: add -repo to repo cache names to not match with deps cache [#288](https://github.com/FundingCircle/jackdaw/pull/288)
* Remove codecov [#289](https://github.com/FundingCircle/jackdaw/pull/289)

## [0.7.10] - 2021-04-14
* Bump netty related packages to latest version (related to changes in `0.7.8` which fixes CVEs).

## [0.7.8] - 2021-03-01
* Override the netty version pulled by Aleph with one which fixes https://nvd.nist.gov/vuln/detail/CVE-2020-11612 [#261](https://github.com/FundingCircle/jackdaw/pull/261)
* Restore the test fixture namespace [#266](https://github.com/FundingCircle/jackdaw/pull/266)

## [0.7.7] - 2021-02-09

### Added

* Added serializer properties when creating an avro serializer

* Exposed deserializer and serializer properties in the serde-resolver.

## [0.7.6] - 2021-07-16

### Added

* Added support for kafka message headers to Test Machine

## [0.7.5] - 2020-07-02

### Fixed

* Replaced deprecated methods in avro.clj  with supported methods.

## [0.7.4] - 2020-04-23

### Fixed

* Fix `ktable` constructor to use supplied `store-name` 

* Bump `clj-uuid` version to `0.1.9`

* Minor update to fix harmless but distracting reflection warnings

## [0.7.3] - 2020-04-08

### Added

 * Allow avro deserializaton via the resolver without a local copy of the schema

 * Start formalizing test-machine commands with fspec'd functions

### Fixed

 * Moved dependency on kafka_2.11 into the dev profile

## [0.7.2] - 2020-02-07

### Fixed

* Fixed bug in Avro deserialisation, when handling a union of enum types

## [0.7.1] - 2020-02-06

### Added

 * as-edn/as-json functions to convert between representations of avro

### Fixed

 * Fixed bug in `map->ProducerRecord`
 * Allow nullable `partition` and `timestamp` in `->ProducerRecord` (previously threw NPE)
 * Fixed union type serialisation when members have similar fields

## [0.7.0] - 2019-12-19

### Added

 * Fressian Serde via clojure.data.fressian #209
 * Clearer error from the command runner #214
 * Documentation about Jackdaw Admin API #211
 * Upgraded in #217:
   * Kafka client version to 2.3.1: https://kafka.apache.org/23/documentation.html#upgrade_230_notable
   * Confluent Platform components version to 5.3.1: https://docs.confluent.io/5.3.1/release-notes/index.html#cp-5-3-1-release-notes
 * Added functions to simplify querying the test machine journal (#215)

## [0.6.9] - 2019-10-16

### Added

 * Upgrade the test-runner (#184)
 * Added support for user-provided parameters to reset-application-fixture (#177)
 * Continuing refinement of examples (#191)
 * Support for `.suppress` (#23)
 * Support for group-options when creating rest-proxy client for the test-machine (#206)
 * PR Template (#187)
 * A new edn serde without the un-necessary newline (#190)

### Fixed

 * Fail fast throwing an exception as soon as a command fails (#186)
 * Throw an exception on unknown commands, useful to detect typos early (#182)
 * Fixed add-key (part of publishing pipeline) (#181)
 * Skip deploy_snapshots job for external contributors (#194)
 * Only numbers should be coercable (#203)
 * Small refactor to implementation of rest-proxy transport (#205)
 * topics-ready? does not dereference the returned deferred (#193)

## [0.6.8] - 2019-08-22

### Fixed

 * Fix test machine status middleware (#157)
 * Fix application reset fixture to use bootstrap servers form app-config (#157)
 * Fix type-hint call to `KafkaAvroDeserializer.deseialize` to remove warning (#157)

## [0.6.7] - 2019-07-30

### Added

 * Allow specification of :deserialization-properties (#157)
 * Back-fill a few tests of jackdaw.client.partitioning (#165)

### Changed

 * Upgrade Clojure version to 1.10.1 (#159)
 * Partitioner in test-machine write command updated to match streams (#139)
 * Reformatted all the code using cljfmt (#173)

### Fixed

 * Delete duplicated tests (#165)
 * Documentation/Examples fixes (#166, #168)
 * Do not assume result of executing command is a map (#164)
 * Supply `key-serde` as well as `value-serde` in `aggregate` methods (#172)

## [0.6.6] - 2019-06-20

### Added

 * Auto-coercion of clojure numbers if possible (#135)
 * Added more explicit information about commit signing Contributing guide (#150)

### Fixed

 * Merger instance required for session window aggregation (#142)
 * Fixed mis-leading parameter names relating to global ktables (#147)
 * Select matching record from union during serialization (#149)
 * Fixed typo in one of the code examples in the streams guide (#151)

## [0.6.5] - 2019-06-14

### Added

 * Add Add `:do!` and `:inspect` test commands (#141)

### Changed

 * Fix regression in multi-topic stream constructor (#143)
 * Stop swallowing errors in test-machine (#144)
 * Include kafka-streams-test-utils so users don't have to (#138)

## [0.6.4] - 2019-05-02

### Added

 * Add changelog and contributing files (#122)
 * Add test-machine example to the word-count example application (#120)
 * Add new arities for aggregate and reduce (#132)
 * Add sign-off to contributing file (#123)

### Changed

 * Ensure user-supplied partitions are cast to `int` before handing
   them to the underlying kafka producer (#124)
 * Make the test for the rest-proxy transport use keywords to identify topics (#120)
 * Resolve dependency conflict reported by lein deps :tree (#125)
 * Fix a typo in the `service-ready?` test fixture (#121)
 * Make sure mock dirver is closed after use (#128)
 * Fix Jackdaw version for Word Count example (#131)
 * Update changelog for 0.6.4 release (#124)

### Removed

 * Delete a couple of overly verbose logging statements (#124)


## [0.6.3] - 2019-03-28

### Added

None

### Changed

 * Upgrade Kafka dependency to 2.2.0 (#123)

### Removed

None


## [0.6.2] - 2019-03-21

### Added

 * Improvement and clarification of documentation (on-going) (#116, #117, #118)
 * Support for including a literal avro schema in the topic definitions resolved by the default resolver (#109)
 * Support for including a custom :partition-fn in kafka streams operations that write records (#103)

### Changed

 * Implement kibit recommendations (#118)
 * Word-count example (#108)
   - Log to file instead of stdout
   - A few simplifications in the implementation
 * In the streams mock driver, get-records now returns a vector of "datafied" producer-records rather than simply the k,v pairs (part of #103)

### Removed

None
