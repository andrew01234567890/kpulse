# syntax=docker/dockerfile:1
# Pulsar broker image with the kpulse Kafka protocol handler NAR baked in, for KIND e2e tests.
ARG PULSAR_VERSION=5.0.0-M1
FROM apachepulsar/pulsar:${PULSAR_VERSION}

# The NAR and e2e-client are staged next to this Dockerfile by the build (see ci.yml / e2e/run-local.sh).
COPY kpulse.nar /pulsar/protocols/kpulse.nar
COPY kpulse-standalone.conf /pulsar/conf/kpulse-standalone.conf

# Real Kafka producer/consumer CLI (org.apache.kafka:kafka-clients) used by assert.sh via `kubectl exec`
# to drive a produce/consume round-trip against the deployed broker.
COPY e2e-client /pulsar/e2e-client
