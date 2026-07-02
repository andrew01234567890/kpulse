package dev.kpulse.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class KafkaTopicNameTest {

    private final KafkaTopicName mapper = new KafkaTopicName("public", "default");

    @Test
    void mapsKafkaTopicToPulsarFullyQualifiedName() {
        assertThat(mapper.toPulsarTopic("orders"))
            .isEqualTo("persistent://public/default/orders");
    }

    @Test
    void leavesAlreadyQualifiedPulsarTopicUnchanged() {
        String qualified = "persistent://tenant-a/ns-b/events";
        assertThat(mapper.toPulsarTopic(qualified)).isEqualTo(qualified);
    }

    @Test
    void extractsShortKafkaTopicFromPulsarName() {
        assertThat(KafkaTopicName.toKafkaTopic("persistent://public/default/orders"))
            .isEqualTo("orders");
        assertThat(KafkaTopicName.toKafkaTopic("orders")).isEqualTo("orders");
    }
}
