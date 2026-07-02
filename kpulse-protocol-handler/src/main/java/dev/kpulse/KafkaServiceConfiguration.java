package dev.kpulse;

import dev.kpulse.common.KafkaListenerEndpoint;
import java.util.List;
import java.util.Properties;
import org.apache.pulsar.broker.ServiceConfiguration;

/**
 * kpulse configuration, sourced from custom {@code kafka*} properties on the broker's
 * {@link ServiceConfiguration}. Listeners are validated eagerly so misconfiguration fails at
 * broker startup rather than on first client connect.
 */
public final class KafkaServiceConfiguration {

    static final String LISTENERS = "kafkaListeners";
    static final String ADVERTISED_LISTENERS = "kafkaAdvertisedListeners";
    static final String TENANT = "kafkaTenant";
    static final String NAMESPACE = "kafkaNamespace";

    static final String DEFAULT_LISTENERS = "PLAINTEXT://127.0.0.1:9092";
    static final String DEFAULT_TENANT = "public";
    static final String DEFAULT_NAMESPACE = "default";

    private final String listeners;
    private final String advertisedListeners;
    private final String tenant;
    private final String namespace;

    private KafkaServiceConfiguration(String listeners, String advertisedListeners,
                                      String tenant, String namespace) {
        this.listeners = listeners;
        this.advertisedListeners = advertisedListeners;
        this.tenant = tenant;
        this.namespace = namespace;
    }

    public static KafkaServiceConfiguration from(ServiceConfiguration conf) {
        Properties p = conf.getProperties();
        String listeners = p.getProperty(LISTENERS, DEFAULT_LISTENERS);
        String advertised = p.getProperty(ADVERTISED_LISTENERS, listeners);
        String tenant = p.getProperty(TENANT, DEFAULT_TENANT);
        String namespace = p.getProperty(NAMESPACE, DEFAULT_NAMESPACE);
        KafkaListenerEndpoint.parseList(listeners);
        KafkaListenerEndpoint.parseList(advertised);
        return new KafkaServiceConfiguration(listeners, advertised, tenant, namespace);
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

    public List<KafkaListenerEndpoint> listenerEndpoints() {
        return KafkaListenerEndpoint.parseList(listeners);
    }
}
