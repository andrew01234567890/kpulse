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
        conf.getProperties().setProperty("kafkaAllowInsecureRemote", "true");

        KafkaProtocolHandler handler = new KafkaProtocolHandler();
        handler.initialize(conf);

        assertThat(handler.getProtocolDataToAdvertise()).isEqualTo("PLAINTEXT://broker-1:9092");
        assertThat(handler.newChannelInitializers()).hasSize(1);
    }

    @Test
    void initializeRejectsSecurityAndAdvertisingModesNotImplementedByM1() {
        ServiceConfiguration tls = configurationWithInterceptor();
        tls.getProperties().setProperty("kafkaListeners", "SSL://127.0.0.1:9093");
        tls.getProperties().setProperty("kafkaAdvertisedListeners", "SSL://broker-1:9093");
        assertThatThrownBy(() -> new KafkaProtocolHandler().initialize(tls))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("PLAINTEXT");

        ServiceConfiguration wildcard = configurationWithInterceptor();
        wildcard.getProperties().setProperty("kafkaListeners", "PLAINTEXT://0.0.0.0:9092");
        wildcard.getProperties().setProperty("kafkaAdvertisedListeners", "PLAINTEXT://0.0.0.0:9092");
        wildcard.getProperties().setProperty("kafkaAllowInsecureRemote", "true");
        assertThatThrownBy(() -> new KafkaProtocolHandler().initialize(wildcard))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("client-resolvable");

        ServiceConfiguration multiple = configurationWithInterceptor();
        multiple.getProperties().setProperty(
            "kafkaListeners", "PLAINTEXT://127.0.0.1:9092,PLAINTEXT://127.0.0.1:9093");
        multiple.getProperties().setProperty("kafkaAdvertisedListeners", "PLAINTEXT://broker-1:9092");
        assertThatThrownBy(() -> new KafkaProtocolHandler().initialize(multiple))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("exactly one");

        ServiceConfiguration remote = configurationWithInterceptor();
        remote.getProperties().setProperty("kafkaListeners", "PLAINTEXT://0.0.0.0:9092");
        remote.getProperties().setProperty("kafkaAdvertisedListeners", "PLAINTEXT://broker-1:9092");
        assertThatThrownBy(() -> new KafkaProtocolHandler().initialize(remote))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("kafkaAllowInsecureRemote=true");

        ServiceConfiguration deceptiveDns = configurationWithInterceptor();
        deceptiveDns.getProperties().setProperty("kafkaListeners", "PLAINTEXT://127.example.com:9092");
        deceptiveDns.getProperties().setProperty("kafkaAdvertisedListeners", "PLAINTEXT://broker-1:9092");
        assertThatThrownBy(() -> new KafkaProtocolHandler().initialize(deceptiveDns))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("kafkaAllowInsecureRemote=true");

        ServiceConfiguration emptyWildcard = configurationWithInterceptor();
        emptyWildcard.getProperties().setProperty("kafkaListeners", "PLAINTEXT://:9092");
        emptyWildcard.getProperties().setProperty("kafkaAdvertisedListeners", "PLAINTEXT://broker-1:9092");
        assertThatThrownBy(() -> new KafkaProtocolHandler().initialize(emptyWildcard))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("wildcard binding");
    }

    private static ServiceConfiguration configurationWithInterceptor() {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setBrokerEntryMetadataInterceptors(Set.of(INTERCEPTOR));
        return conf;
    }
}
