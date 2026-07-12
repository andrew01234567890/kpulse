package dev.kpulse;

import dev.kpulse.common.KafkaListenerEndpoint;
import dev.kpulse.common.KafkaTopicName;
import dev.kpulse.format.KafkaEntryFormatter;
import dev.kpulse.storage.KafkaTopicManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.kafka.common.Node;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Per-broker singleton shared by every connection: the advertised self-node, the topic mapper, and
 * the produce/fetch machinery. Built once in {@link KafkaProtocolHandler#start}.
 */
public final class KafkaRequestContext {

    private static final int MAX_TOPIC_DISCOVERY_CANDIDATES = 10_000;
    private static final int TOPIC_DISCOVERY_CONCURRENCY = 16;

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

    public CompletableFuture<List<String>> listKafkaTopics() {
        String namespace = config.getKafkaTenant() + "/" + config.getKafkaNamespace();
        try {
            return brokerService.getPulsar().getAdminClient().topics()
                .getListAsync(namespace, TopicDomain.persistent)
                .thenCompose(this::filterKafkaTopics);
        } catch (Exception error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    private CompletableFuture<List<String>> filterKafkaTopics(List<String> topics) {
        List<TopicName> candidates = topics.stream()
            .map(TopicName::get)
            .filter(topic -> topic.getDomain() == TopicDomain.persistent)
            .filter(topic -> topic.getPartitionIndex() < 0)
            .filter(this::isValidMappedTopic)
            .distinct()
            .toList();
        if (candidates.size() > MAX_TOPIC_DISCOVERY_CANDIDATES) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                "Kafka topic discovery candidate limit exceeded: " + candidates.size()));
        }

        return inspectKafkaTopics(candidates, TOPIC_DISCOVERY_CONCURRENCY,
            topic -> topicManager.isKafkaTopic(topic.toString()));
    }

    static CompletableFuture<List<String>> inspectKafkaTopics(
            List<TopicName> candidates, int concurrency,
            Function<TopicName, CompletableFuture<Boolean>> markerCheck) {
        if (concurrency < 1) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("Topic discovery concurrency must be positive"));
        }
        List<String> kafkaTopics = Collections.synchronizedList(new ArrayList<>());
        int workerCount = Math.min(concurrency, candidates.size());
        CompletableFuture<?>[] workers = new CompletableFuture<?>[workerCount];
        for (int worker = 0; worker < workerCount; worker++) {
            CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
            for (int candidate = worker; candidate < candidates.size(); candidate += workerCount) {
                TopicName topic = candidates.get(candidate);
                chain = chain.thenCompose(ignored -> markerCheck.apply(topic))
                    .thenAccept(isKafkaTopic -> {
                        if (isKafkaTopic) {
                            kafkaTopics.add(topic.getLocalName());
                        }
                    });
            }
            workers[worker] = chain;
        }
        return CompletableFuture.allOf(workers).thenApply(ignored -> kafkaTopics.stream()
            .distinct()
            .sorted()
            .toList());
    }

    private boolean isValidMappedTopic(TopicName topicName) {
        try {
            return topicName.toString().equals(topicMapper.toPulsarTopic(topicName.getLocalName()));
        } catch (IllegalArgumentException error) {
            return false;
        }
    }

    private static KafkaListenerEndpoint advertisedEndpoint(KafkaServiceConfiguration config) {
        return KafkaListenerEndpoint.parseList(config.getKafkaAdvertisedListeners()).get(0);
    }
}
