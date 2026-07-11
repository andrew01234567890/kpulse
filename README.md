# kpulse

A **Kafka protocol handler for Apache Pulsar**. `kpulse` is a Pulsar `ProtocolHandler` plugin
(packaged as a `.nar`) whose goal is **maximum Kafka client compatibility**.

> **Status: early (M1).** ApiVersions, Metadata, Produce, Fetch, and ListOffsets support a
> single broker and partition 0. Consumer groups, multi-broker routing, additional partitions,
> admin APIs, transactions, SASL/TLS, authorization, and schema registry are not implemented yet.
> See the milestone list below.

## Targets

| Component | Version |
|-----------|---------|
| Apache Pulsar | `5.0.0-M1` |
| Oxia metadata server | `0.16.7` (Pulsar M1 bundles client `0.8.0`) |
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
   kafkaAllowInsecureRemote=true
   ```

   The `brokerEntryMetadataInterceptors` entry is **required**: kpulse maps Kafka offsets onto
   Pulsar's broker-entry index, and refuses to start without it rather than silently corrupt
   consumer positions.

   **Security warning:** M1 has no Kafka authentication or Pulsar authorization. Remote binding is
   rejected by default; `kafkaAllowInsecureRemote=true` is an explicit opt-in intended only for an
   isolated, trusted network. Do not expose port 9092 to the public internet.

   M1 is limited to a single broker owning the namespace's topics. It does not yet resolve Pulsar
   bundle ownership or advertise remote owners, so do not deploy it as a multi-broker Kafka endpoint.

### Upgrading topics created before the M1 format marker

Current kpulse releases mark each backing Pulsar topic with
`kpulse.entry.format=kafka`. Topics created by an earlier kpulse build can still be migrated
automatically when a client requests them by name, but Kafka pattern subscriptions and
`listTopics()` cannot discover an unmarked topic after a cold restart.

Before upgrading, explicitly mark every existing topic that was written exclusively through
kpulse. Do not run this on a native or mixed-format Pulsar topic:

```bash
pulsar-admin topics update-properties \
  --property kpulse.entry.format=kafka \
  persistent://public/default/<existing-kpulse-topic>

pulsar-admin topics get-properties \
  persistent://public/default/<existing-kpulse-topic>
```

Repeat the command for each existing kpulse topic before clients use pattern discovery. Named
Produce, Fetch, Metadata, or ListOffsets access remains a fallback that validates the first legacy
entry before adding the marker.

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
