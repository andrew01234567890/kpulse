# kpulse

A **Kafka protocol handler for Apache Pulsar**. `kpulse` is a Pulsar `ProtocolHandler` plugin
(packaged as a `.nar`) that lets unmodified Kafka clients — the Java client, librdkafka,
Kafka Streams, admin and console tools — produce to and consume from an Apache Pulsar broker as if
it were a Kafka cluster. The goal is **maximum Kafka client compatibility**.

> **Status: early.** This is the M0 scaffold — the plugin loads, parses configuration, opens its
> Kafka listeners, and enforces its broker prerequisites. Per-API request handling (ApiVersions,
> Metadata, Produce, Fetch, consumer groups, offsets, admin, transactions, SASL, schema registry)
> lands milestone-by-milestone. See the milestone list below.

## Targets

| Component | Version |
|-----------|---------|
| Apache Pulsar | `5.0.0-M1` |
| Apache Kafka (protocol baseline) | `4.3.1` |
| Java | 17+ (built on JDK 21) |
| Build | Gradle (via the committed `./gradlew` wrapper) |

## Build

No system Gradle or Maven is required — the wrapper bootstraps everything:

```bash
./gradlew build          # compile, run unit + integration tests
./gradlew :kpulse-protocol-handler:nar   # assemble the protocol-handler NAR
```

The NAR is written to `kpulse-protocol-handler/build/libs/kpulse-protocol-handler-*.nar`.

## Deploying into a broker

1. Copy the NAR into the broker's `protocolHandlerDirectory` (default `./protocols`).
2. Set the following in `broker.conf` (or the equivalent Helm values):

   ```properties
   messagingProtocols=kafka
   brokerEntryMetadataInterceptors=org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor
   kafkaListeners=PLAINTEXT://0.0.0.0:9092
   kafkaAdvertisedListeners=PLAINTEXT://<client-resolvable-host>:9092
   ```

   The `brokerEntryMetadataInterceptors` entry is **required**: kpulse maps Kafka offsets onto
   Pulsar's broker-entry index, and refuses to start without it rather than silently corrupt
   consumer positions.

## Module layout

- `kpulse-common` — Kafka↔Pulsar name mapping, listener parsing, shared codec adapters.
- `kpulse-protocol-handler` — the `ProtocolHandler` implementation, Netty pipeline, and per-API
  handlers; builds the NAR.
- `kpulse-schema-registry` — Confluent-compatible schema registry HTTP API (later milestone).
- `kpulse-tests` — embedded-broker integration tests driven by real Kafka clients.

## Milestones

M0 scaffold → M1 vertical slice (ApiVersions/Metadata/Produce/Fetch) → M2 consumer groups →
M3 offset storage → M4 admin/topic management → M5 KIP-848 group protocol → M6 SASL + TLS →
M7 transactions/EOS → M8 schema registry.

## Contributing

Every change lands via a pull request (the `main` branch is protected):

- **PR title** must be `[major|minor|patch] Descriptive Title` — the bump level drives the
  release-only semantic version.
- **PR description** is required.
- All CI checks (build + unit/integration, KIND e2e, PR-title) must pass before merge.
- Merges are **squash-only**; a version tag + GitHub Release are cut automatically on merge to
  `main` (never on PR builds).

## License

Apache License 2.0 — see [LICENSE](LICENSE).
