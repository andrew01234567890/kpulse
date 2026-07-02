package dev.kpulse.common;

import java.util.Objects;

/**
 * Bidirectional mapping between Kafka topic names and Pulsar fully-qualified topic names.
 *
 * <p>A Kafka topic {@code my-topic} maps to {@code persistent://<tenant>/<namespace>/my-topic}.
 */
public final class KafkaTopicName {

    private static final String PERSISTENT_DOMAIN = "persistent://";

    private final String tenant;
    private final String namespace;

    public KafkaTopicName(String tenant, String namespace) {
        this.tenant = Objects.requireNonNull(tenant, "tenant");
        this.namespace = Objects.requireNonNull(namespace, "namespace");
    }

    /** Translate a Kafka topic name to its Pulsar fully-qualified name. */
    public String toPulsarTopic(String kafkaTopic) {
        Objects.requireNonNull(kafkaTopic, "kafkaTopic");
        if (kafkaTopic.startsWith(PERSISTENT_DOMAIN)) {
            return kafkaTopic;
        }
        return PERSISTENT_DOMAIN + tenant + "/" + namespace + "/" + kafkaTopic;
    }

    /** Extract the short Kafka topic name from a Pulsar fully-qualified name. */
    public static String toKafkaTopic(String pulsarTopic) {
        Objects.requireNonNull(pulsarTopic, "pulsarTopic");
        String stripped = pulsarTopic.startsWith(PERSISTENT_DOMAIN)
            ? pulsarTopic.substring(PERSISTENT_DOMAIN.length())
            : pulsarTopic;
        int lastSlash = stripped.lastIndexOf('/');
        return lastSlash >= 0 ? stripped.substring(lastSlash + 1) : stripped;
    }
}
