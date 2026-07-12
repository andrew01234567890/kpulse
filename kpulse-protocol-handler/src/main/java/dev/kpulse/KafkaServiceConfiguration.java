package dev.kpulse;

import dev.kpulse.common.KafkaListenerEndpoint;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.pulsar.broker.ServiceConfiguration;

/**
 * kpulse configuration, sourced from custom {@code kafka*} properties on the broker's
 * {@link ServiceConfiguration}. Listeners are validated eagerly so misconfiguration fails at
 * broker startup rather than on first client connect.
 */
public final class KafkaServiceConfiguration {

    private static final String SUPPORTED_SECURITY_PROTOCOL = "PLAINTEXT";

    static final String LISTENERS = "kafkaListeners";
    static final String ADVERTISED_LISTENERS = "kafkaAdvertisedListeners";
    static final String TENANT = "kafkaTenant";
    static final String NAMESPACE = "kafkaNamespace";
    static final String ALLOW_INSECURE_REMOTE = "kafkaAllowInsecureRemote";

    static final String DEFAULT_LISTENERS = "PLAINTEXT://127.0.0.1:9092";
    static final String DEFAULT_TENANT = "public";
    static final String DEFAULT_NAMESPACE = "default";

    private final String listeners;
    private final String advertisedListeners;
    private final String tenant;
    private final String namespace;
    private final boolean allowInsecureRemote;

    private KafkaServiceConfiguration(String listeners, String advertisedListeners,
                                      String tenant, String namespace, boolean allowInsecureRemote) {
        this.listeners = listeners;
        this.advertisedListeners = advertisedListeners;
        this.tenant = tenant;
        this.namespace = namespace;
        this.allowInsecureRemote = allowInsecureRemote;
    }

    public static KafkaServiceConfiguration from(ServiceConfiguration conf) {
        Properties p = conf.getProperties();
        String listeners = p.getProperty(LISTENERS, DEFAULT_LISTENERS);
        String advertised = p.getProperty(ADVERTISED_LISTENERS, listeners);
        String tenant = p.getProperty(TENANT, DEFAULT_TENANT);
        String namespace = p.getProperty(NAMESPACE, DEFAULT_NAMESPACE);
        boolean allowInsecureRemote = parseBoolean(p, ALLOW_INSECURE_REMOTE, false);
        List<KafkaListenerEndpoint> listenerEndpoints = KafkaListenerEndpoint.parseList(listeners);
        List<KafkaListenerEndpoint> advertisedEndpoints = KafkaListenerEndpoint.parseList(advertised);
        requireSinglePlaintextEndpoint(LISTENERS, listenerEndpoints, false, allowInsecureRemote);
        requireSinglePlaintextEndpoint(ADVERTISED_LISTENERS, advertisedEndpoints, true, allowInsecureRemote);
        return new KafkaServiceConfiguration(
            listeners, advertised, tenant, namespace, allowInsecureRemote);
    }

    public String getKafkaListeners() {
        return listeners;
    }

    public String getKafkaAdvertisedListeners() {
        return advertisedListeners;
    }

    public String getKafkaTenant() {
        return tenant;
    }

    public String getKafkaNamespace() {
        return namespace;
    }

    public boolean isAllowInsecureRemote() {
        return allowInsecureRemote;
    }

    public List<KafkaListenerEndpoint> listenerEndpoints() {
        return KafkaListenerEndpoint.parseList(listeners);
    }

    private static void requireSinglePlaintextEndpoint(
            String property, List<KafkaListenerEndpoint> endpoints, boolean advertised,
            boolean allowInsecureRemote) {
        if (endpoints.size() != 1) {
            throw new IllegalArgumentException(
                property + " currently requires exactly one endpoint, got " + endpoints.size());
        }
        KafkaListenerEndpoint endpoint = endpoints.get(0);
        if (!SUPPORTED_SECURITY_PROTOCOL.equalsIgnoreCase(endpoint.securityProtocol())) {
            throw new IllegalArgumentException(
                property + " supports only PLAINTEXT until TLS/SASL is implemented, got "
                    + endpoint.securityProtocol());
        }
        if (advertised && isWildcardHost(endpoint.host())) {
            throw new IllegalArgumentException(
                property + " must use a client-resolvable host, got " + endpoint.host());
        }
        if (!advertised && isWildcardHost(endpoint.host()) && !allowInsecureRemote) {
            throw new IllegalArgumentException(
                property + " wildcard binding requires " + ALLOW_INSECURE_REMOTE
                    + "=true because authentication/authorization is not implemented");
        }
        if (!advertised && !allowInsecureRemote && !isLoopbackHost(endpoint.host())) {
            throw new IllegalArgumentException(
                property + " may bind only to loopback until authentication/authorization is implemented; "
                    + "set " + ALLOW_INSECURE_REMOTE + "=true only for an isolated trusted network");
        }
    }

    private static boolean isWildcardHost(String host) {
        return host.isEmpty() || host.equals("0.0.0.0") || host.equals("::") || host.equals("[::]");
    }

    private static boolean isLoopbackHost(String host) {
        String resolvableHost = host.startsWith("[") && host.endsWith("]")
            ? host.substring(1, host.length() - 1) : host;
        try {
            InetAddress[] addresses = InetAddress.getAllByName(resolvableHost);
            return addresses.length > 0 && Arrays.stream(addresses).allMatch(InetAddress::isLoopbackAddress);
        } catch (UnknownHostException error) {
            return false;
        }
    }

    private static boolean parseBoolean(Properties properties, String name, boolean defaultValue) {
        String value = properties.getProperty(name);
        if (value == null) {
            return defaultValue;
        }
        if (value.equalsIgnoreCase("true")) {
            return true;
        }
        if (value.equalsIgnoreCase("false")) {
            return false;
        }
        throw new IllegalArgumentException(name + " must be true or false, got: " + value);
    }
}
