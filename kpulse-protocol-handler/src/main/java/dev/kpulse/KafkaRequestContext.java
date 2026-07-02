package dev.kpulse;

import dev.kpulse.common.KafkaListenerEndpoint;
import dev.kpulse.common.KafkaTopicName;
import dev.kpulse.format.KafkaEntryFormatter;
import dev.kpulse.storage.KafkaTopicManager;
import org.apache.kafka.common.Node;
import org.apache.pulsar.broker.service.BrokerService;

/**
 * Per-broker singleton shared by every connection: the advertised self-node, the topic mapper, and
 * the produce/fetch machinery. Built once in {@link KafkaProtocolHandler#start}.
 */
public final class KafkaRequestContext {

    static final int SELF_NODE_ID = 1;

    private final BrokerService brokerService;
    private final KafkaServiceConfiguration config;
    private final Node selfNode;
    private final String clusterId;
    private final KafkaTopicName topicMapper;
    private final KafkaTopicManager topicManager;

    public KafkaRequestContext(BrokerService brokerService, KafkaServiceConfiguration config, String clusterId) {
        this.brokerService = brokerService;
        this.config = config;
        this.clusterId = clusterId;
        KafkaListenerEndpoint advertised = advertisedEndpoint(config);
        this.selfNode = new Node(SELF_NODE_ID, advertised.host(), advertised.port());
        this.topicMapper = new KafkaTopicName(config.getKafkaTenant(), config.getKafkaNamespace());
        this.topicManager = new KafkaTopicManager(brokerService, new KafkaEntryFormatter());
    }

    public BrokerService brokerService() {
        return brokerService;
    }

    public KafkaServiceConfiguration config() {
        return config;
    }

    public Node selfNode() {
        return selfNode;
    }

    public String clusterId() {
        return clusterId;
    }

    public KafkaTopicName topicMapper() {
        return topicMapper;
    }

    public KafkaTopicManager topicManager() {
        return topicManager;
    }

    private static KafkaListenerEndpoint advertisedEndpoint(KafkaServiceConfiguration config) {
        for (KafkaListenerEndpoint endpoint : KafkaListenerEndpoint.parseList(config.getKafkaAdvertisedListeners())) {
            if (!endpoint.host().isEmpty()) {
                return endpoint;
            }
        }
        // Advertising a wildcard host would tell clients to reconnect to 0.0.0.0; fall back to loopback.
        KafkaListenerEndpoint wildcard = KafkaListenerEndpoint.parseList(config.getKafkaAdvertisedListeners()).get(0);
        return new KafkaListenerEndpoint(wildcard.securityProtocol(), "127.0.0.1", wildcard.port());
    }
}
