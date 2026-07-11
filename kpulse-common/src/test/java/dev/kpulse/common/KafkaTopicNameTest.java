package dev.kpulse.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class KafkaTopicNameTest {

    private final KafkaTopicName mapper = new KafkaTopicName("public", "default");

    @Test
    void mapsKafkaTopicToPulsarFullyQualifiedName() {
        assertThat(mapper.toPulsarTopic("orders"))
            .isEqualTo("persistent://public/default/orders");
    }

    @Test
    void rejectsQualifiedPulsarTopicThatEscapesConfiguredNamespace() {
        String qualified = "persistent://tenant-a/ns-b/events";
        assertThatThrownBy(() -> mapper.toPulsarTopic(qualified))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsKafkaTopicNamesThatKafkaDoesNotAllow() {
        assertThatThrownBy(() -> mapper.toPulsarTopic("tenant/namespace/topic"))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> mapper.toPulsarTopic(".."))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void extractsShortKafkaTopicFromPulsarName() {
        assertThat(KafkaTopicName.toKafkaTopic("persistent://public/default/orders"))
            .isEqualTo("orders");
        assertThat(KafkaTopicName.toKafkaTopic("orders")).isEqualTo("orders");
    }
}
