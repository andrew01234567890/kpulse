package dev.kpulse;

import dev.kpulse.common.KafkaListenerEndpoint;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kpulse Kafka {@link ProtocolHandler}: lets Kafka clients speak to an Apache Pulsar broker.
 *
 * <p>M0 establishes the plugin lifecycle, configuration, and Netty listeners. Per-API request
 * handling (ApiVersions, Metadata, Produce, Fetch, consumer groups, ...) is added from M1 onward.
 */
public class KafkaProtocolHandler implements ProtocolHandler {

    public static final String PROTOCOL_NAME = "kafka";

    static final String REQUIRED_INTERCEPTOR =
        "org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor";

    private static final Logger log = LoggerFactory.getLogger(KafkaProtocolHandler.class);

    private static final String DEFAULT_CLUSTER_ID = "kpulse";

    private KafkaServiceConfiguration config;
    private KafkaRequestContext requestContext;

    @Override
    public String protocolName() {
        return PROTOCOL_NAME;
    }

    @Override
    public boolean accept(String protocol) {
        return PROTOCOL_NAME.equalsIgnoreCase(protocol);
    }

    @Override
    public void initialize(ServiceConfiguration conf) throws Exception {
        requireBrokerEntryMetadataInterceptor(conf);
        this.config = KafkaServiceConfiguration.from(conf);
        log.info("Initialized kpulse: listeners={}, advertised={}, tenant={}, namespace={}",
            config.getKafkaListeners(), config.getKafkaAdvertisedListeners(),
            config.getKafkaTenant(), config.getKafkaNamespace());
        if (config.isAllowInsecureRemote()) {
            log.warn("kpulse remote PLAINTEXT access is explicitly enabled without authentication or authorization");
        }
    }

    @Override
    public String getProtocolDataToAdvertise() {
        return config == null ? null : config.getKafkaAdvertisedListeners();
    }

    @Override
    public void start(BrokerService service) {
        log.info("Starting kpulse Kafka protocol handler on {}", config.getKafkaListeners());
        String clusterId = service.getPulsar().getConfiguration().getClusterName();
        this.requestContext = new KafkaRequestContext(
            service, config, clusterId != null ? clusterId : DEFAULT_CLUSTER_ID);
        // Group/transaction coordinators and auth are wired in later milestones.
    }

    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        Map<InetSocketAddress, ChannelInitializer<SocketChannel>> initializers = new LinkedHashMap<>();
        for (KafkaListenerEndpoint endpoint : config.listenerEndpoints()) {
            initializers.put(endpoint.toSocketAddress(), new KafkaChannelInitializer(requestContext));
        }
        return initializers;
    }

    @Override
    public void close() {
        log.info("Closing kpulse Kafka protocol handler");
    }

    /**
     * kpulse maps Kafka offsets onto Pulsar's broker-entry index, which the broker only stamps when
     * {@code AppendIndexMetadataInterceptor} is enabled. Without it, offsets would silently read as
     * zero, so refuse to start rather than corrupt consumer positions.
     */
    private static void requireBrokerEntryMetadataInterceptor(ServiceConfiguration conf) {
        if (!conf.getBrokerEntryMetadataInterceptors().contains(REQUIRED_INTERCEPTOR)) {
            throw new IllegalStateException(
                "kpulse requires broker config 'brokerEntryMetadataInterceptors' to include "
                    + REQUIRED_INTERCEPTOR + " so Kafka offsets map to a stable broker-entry index. "
                    + "Configured interceptors: " + conf.getBrokerEntryMetadataInterceptors());
        }
    }
}
