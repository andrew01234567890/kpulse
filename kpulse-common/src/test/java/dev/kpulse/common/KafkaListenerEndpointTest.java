package dev.kpulse.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class KafkaListenerEndpointTest {

    @Test
    void parsesProtocolHostAndPort() {
        KafkaListenerEndpoint endpoint = KafkaListenerEndpoint.parse("PLAINTEXT://broker-1:9092");
        assertThat(endpoint.securityProtocol()).isEqualTo("PLAINTEXT");
        assertThat(endpoint.host()).isEqualTo("broker-1");
        assertThat(endpoint.port()).isEqualTo(9092);
        assertThat(endpoint.toSocketAddress().getPort()).isEqualTo(9092);
    }

    @Test
    void parsesMultipleCommaSeparatedListeners() {
        assertThat(KafkaListenerEndpoint.parseList("PLAINTEXT://:9092, SSL://host:9093"))
            .hasSize(2);
    }

    @Test
    void treatsEmptyHostAsWildcardBind() {
        KafkaListenerEndpoint endpoint = KafkaListenerEndpoint.parse("PLAINTEXT://:9092");
        assertThat(endpoint.host()).isEmpty();
        assertThat(endpoint.toSocketAddress().getAddress().isAnyLocalAddress()).isTrue();
    }

    @Test
    void rejectsListenerWithoutProtocol() {
        assertThatThrownBy(() -> KafkaListenerEndpoint.parse("host:9092"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsListenerWithNonNumericPort() {
        assertThatThrownBy(() -> KafkaListenerEndpoint.parse("PLAINTEXT://host:kafka"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsBlankProtocolAndOutOfRangePort() {
        assertThatThrownBy(() -> KafkaListenerEndpoint.parse("://host:9092"))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaListenerEndpoint.parse("PLAINTEXT://host:0"))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaListenerEndpoint.parse("PLAINTEXT://host:65536"))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
