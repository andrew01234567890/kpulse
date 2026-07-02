package dev.kpulse.common;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A parsed Kafka listener endpoint such as {@code PLAINTEXT://0.0.0.0:9092}.
 */
public final class KafkaListenerEndpoint {

    private final String securityProtocol;
    private final String host;
    private final int port;

    public KafkaListenerEndpoint(String securityProtocol, String host, int port) {
        this.securityProtocol = Objects.requireNonNull(securityProtocol, "securityProtocol");
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
    }

    public String securityProtocol() {
        return securityProtocol;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public InetSocketAddress toSocketAddress() {
        // An empty host means bind to all interfaces (Kafka's "PLAINTEXT://:9092" wildcard form).
        return host.isEmpty() ? new InetSocketAddress(port) : new InetSocketAddress(host, port);
    }

    /** Parse a comma-separated list of listeners, e.g. {@code PLAINTEXT://:9092,SSL://:9093}. */
    public static List<KafkaListenerEndpoint> parseList(String listeners) {
        Objects.requireNonNull(listeners, "listeners");
        List<KafkaListenerEndpoint> result = new ArrayList<>();
        for (String raw : listeners.split(",")) {
            String entry = raw.trim();
            if (!entry.isEmpty()) {
                result.add(parse(entry));
            }
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException("No valid Kafka listeners in: " + listeners);
        }
        return result;
    }

    /** Parse a single {@code PROTOCOL://host:port} listener. */
    public static KafkaListenerEndpoint parse(String listener) {
        int sep = listener.indexOf("://");
        if (sep < 0) {
            throw new IllegalArgumentException(
                "Listener must be PROTOCOL://host:port, got: " + listener);
        }
        String protocol = listener.substring(0, sep);
        String hostPort = listener.substring(sep + 3);
        int colon = hostPort.lastIndexOf(':');
        if (colon < 0) {
            throw new IllegalArgumentException("Listener must include a port: " + listener);
        }
        // Host may be empty ("PLAINTEXT://:9092") — that is Kafka's wildcard/all-interfaces form.
        String host = hostPort.substring(0, colon);
        int port;
        try {
            port = Integer.parseInt(hostPort.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port in listener: " + listener, e);
        }
        return new KafkaListenerEndpoint(protocol, host, port);
    }
}
