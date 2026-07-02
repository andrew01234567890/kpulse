package dev.kpulse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.junit.jupiter.api.Test;

class KafkaProtocolHandlerTest {

    private static final String INTERCEPTOR =
        "org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor";

    @Test
    void advertisesKafkaProtocolName() {
        KafkaProtocolHandler handler = new KafkaProtocolHandler();
        assertThat(handler.protocolName()).isEqualTo("kafka");
        assertThat(handler.accept("kafka")).isTrue();
        assertThat(handler.accept("KAFKA")).isTrue();
        assertThat(handler.accept("pulsar")).isFalse();
    }

    @Test
    void initializeFailsWithoutBrokerEntryIndexInterceptor() {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.getProperties().setProperty("kafkaListeners", "PLAINTEXT://127.0.0.1:9092");

        KafkaProtocolHandler handler = new KafkaProtocolHandler();
        assertThatThrownBy(() -> handler.initialize(conf))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("AppendIndexMetadataInterceptor");
    }

    @Test
    void initializeSucceedsWithInterceptorAndAdvertisesListeners() throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setBrokerEntryMetadataInterceptors(Set.of(INTERCEPTOR));
        conf.getProperties().setProperty("kafkaListeners", "PLAINTEXT://0.0.0.0:9092");
        conf.getProperties().setProperty("kafkaAdvertisedListeners", "PLAINTEXT://broker-1:9092");

        KafkaProtocolHandler handler = new KafkaProtocolHandler();
        handler.initialize(conf);

        assertThat(handler.getProtocolDataToAdvertise()).isEqualTo("PLAINTEXT://broker-1:9092");
        assertThat(handler.newChannelInitializers()).hasSize(1);
    }
}
