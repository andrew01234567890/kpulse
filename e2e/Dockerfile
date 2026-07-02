# syntax=docker/dockerfile:1
# Pulsar broker image with the kpulse Kafka protocol handler NAR baked in, for KIND e2e tests.
ARG PULSAR_VERSION=5.0.0-M1
FROM apachepulsar/pulsar:${PULSAR_VERSION}

# The NAR is staged next to this Dockerfile by the build (see kind-e2e.yml / e2e/run-local.sh).
COPY kpulse.nar /pulsar/protocols/kpulse.nar
COPY kpulse-standalone.conf /pulsar/conf/kpulse-standalone.conf
