# jackdaw

[![Clojars Project](https://img.shields.io/clojars/v/net.clojars.savya/jackdaw.svg)](https://clojars.org/net.clojars.savya/jackdaw)
[![cljdoc](https://cljdoc.org/badge/net.clojars.savya/jackdaw)](https://cljdoc.org/d/net.clojars.savya/jackdaw/CURRENT)
[![test](https://github.com/jsavyasachi/jackdaw/actions/workflows/test.yml/badge.svg)](https://github.com/jsavyasachi/jackdaw/actions/workflows/test.yml)
[![Renovate](https://img.shields.io/badge/Renovate-enabled-1A1F6C?style=flat&logo=renovate&logoColor=fff)](https://github.com/jsavyasachi/jackdaw/issues?q=is%3Aissue+Dependency+Dashboard)

Jackdaw is a Clojure library for the Apache Kafka distributed streaming platform. With Jackdaw, you can create and list topics with the AdminClient API. You can produce and consume records with the Producer and Consumer APIs. You can create stream processing applications with the Streams API. Jackdaw also contains functions to serialize and deserialize records as JSON, EDN, and Avro, and functions to write unit tests and integration tests.

> **Maintenance fork.** This is a maintained continuation of
> [`fundingcircle/jackdaw`](https://github.com/fundingcircle/jackdaw) (unmaintained
> since 2024), modernized for **Apache Kafka 4.x**. It is published under a new
> coordinate, `net.clojars.savya/jackdaw`. See the [CHANGELOG](CHANGELOG.md) for the
> 4.x migration notes.

## Stack

<a href="https://clojure.org"><img src="https://img.shields.io/badge/Clojure-5881D8?style=flat&logo=clojure&logoColor=fff" alt="Clojure" /></a>
<a href="https://kafka.apache.org"><img src="https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat&logo=apachekafka&logoColor=fff" alt="Apache Kafka" /></a>

## Installation

Leiningen / Boot:

```clojure
[net.clojars.savya/jackdaw "1.4.0"]
```

deps.edn:

```clojure
net.clojars.savya/jackdaw {:mvn/version "1.4.0"}
```

Jackdaw resolves Confluent artifacts from the Confluent Maven repository. Add
`https://packages.confluent.io/maven/` to your `:repositories` (Leiningen) or
`:mvn/repos` (deps.edn).

## Supported versions

Jackdaw 1.3.6 requires **Clojure >= 1.10**, **JDK 17+**, and **Apache Kafka 4.x** /
**Confluent Platform 8.x** brokers. (The `datafy` protocol sets the Clojure floor. Clojure
1.10 introduced that protocol.)

## Documentation

You can find all the documentation on [cljdoc](https://cljdoc.org/d/net.clojars.savya/jackdaw).

The [production patterns guide](doc/production.md) covers transactional producers,
SASL/SSL configuration, interactive state-store queries, graceful Streams shutdown,
and error handling.

## Examples

- [Pipe](https://github.com/jsavyasachi/jackdaw/tree/main/examples/pipe)
- [Word Count](https://github.com/jsavyasachi/jackdaw/tree/main/examples/word-count)
- [Simple Ledger](https://github.com/jsavyasachi/jackdaw/tree/main/examples/simple-ledger)
- [Roll Dice](https://github.com/jsavyasachi/jackdaw/tree/main/examples/rolldice)

## Contributing

We welcome any thoughts or patches - [open an issue](https://github.com/jsavyasachi/jackdaw/issues) on this fork.

Run the broker-free unit suite:

```sh
clojure -M:test
```

Run the full suite against Kafka, schema registry, and Kafka REST Proxy:

```sh
docker compose up -d --wait
clojure -M:test --no-config
docker compose down -v
```

Run only tests tagged as integration against that stack:

```sh
clojure -M:test --no-config --focus-meta :integration
```

## Related projects

To get more information about your topologies, use the
[Topology Grapher](https://github.com/FundingCircle/topology-grapher) library to generate graphs.
See [an example using jackdaw](https://github.com/FundingCircle/topology-grapher/blob/master/sample_project/src/jackdaw_topology.clj) for how to add it to your topology.

## Releasing

This fork self-publishes to Clojars under `net.clojars.savya/jackdaw`.

1. Bump the version in `build.clj` and `project.clj`, then add a dated entry to `CHANGELOG.md`.
2. Run `clojure -M:test` and `clojure -T:build jar`.
3. Deploy: `CLOJARS_USERNAME=<user> CLOJARS_PASSWORD=<clojars-deploy-token> clojure -T:build deploy`.
4. Tag the release: `git tag v<version> && git push --tags`.

## License

Copyright © 2017 Funding Circle

Maintenance fork (2026) by [Savyasachi](https://github.com/jsavyasachi); original:
https://github.com/fundingcircle/jackdaw

Distributed under the BSD 3-Clause License.
 
