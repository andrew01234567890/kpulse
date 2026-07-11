package dev.kpulse.storage;

import dev.kpulse.format.KafkaEntryFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;

/** Resolves and caches one {@link PartitionLog} per Pulsar topic. */
public final class KafkaTopicManager {

    private static final String KAFKA_TOPIC_PROPERTY = "kpulse.entry.format";
    private static final String KAFKA_TOPIC_PROPERTY_VALUE = "kafka";

    private final BrokerService brokerService;
    private final KafkaEntryFormatter formatter;
    private final ConcurrentHashMap<String, PartitionLog> logs = new ConcurrentHashMap<>();

    public KafkaTopicManager(BrokerService brokerService, KafkaEntryFormatter formatter) {
        this.brokerService = brokerService;
        this.formatter = formatter;
    }

    public CompletableFuture<PartitionLog> getPartitionLog(String pulsarTopic) {
        return getPartitionLog(pulsarTopic, true);
    }

    public CompletableFuture<PartitionLog> getPartitionLog(String pulsarTopic, boolean createIfMissing) {
        PartitionLog cached = logs.get(pulsarTopic);
        if (cached != null) {
            return CompletableFuture.completedFuture(cached);
        }
        return createPartitionLog(pulsarTopic, createIfMissing).thenApply(created -> {
            PartitionLog existing = logs.putIfAbsent(pulsarTopic, created);
            return existing != null ? existing : created;
        });
    }

    public static Map<String, String> kafkaTopicProperties() {
        return Map.of(KAFKA_TOPIC_PROPERTY, KAFKA_TOPIC_PROPERTY_VALUE);
    }

    /**
     * Checks the persisted managed-ledger marker without opening or loading the topic. This is used
     * for all-topics Metadata requests, where loading every Pulsar topic would be an easy denial of
     * service vector.
     */
    public CompletableFuture<Boolean> isKafkaTopic(String pulsarTopic) {
        if (logs.containsKey(pulsarTopic)) {
            return CompletableFuture.completedFuture(true);
        }
        TopicName topicName = TopicName.get(pulsarTopic);
        return brokerService.getManagedLedgerFactoryForTopic(topicName)
            .thenCompose(factory -> factory.getManagedLedgerPropertiesAsync(
                topicName.getPersistenceNamingEncoding()))
            .thenApply(properties ->
                KAFKA_TOPIC_PROPERTY_VALUE.equals(properties.get(KAFKA_TOPIC_PROPERTY)));
    }

    private CompletableFuture<PartitionLog> createPartitionLog(String pulsarTopic, boolean createIfMissing) {
        CompletableFuture<Topic> topicFuture = createIfMissing
            ? brokerService.isAllowAutoTopicCreationAsync(pulsarTopic)
                .thenCompose(allowed -> loadTopic(pulsarTopic, allowed))
            : loadTopic(pulsarTopic, false);
        return topicFuture.thenCompose(topic -> {
            if (!(topic instanceof PersistentTopic persistentTopic)) {
                return CompletableFuture.failedFuture(
                    new IllegalStateException("kpulse supports only persistent topics, got: " + pulsarTopic));
            }
            return ensureKafkaTopicMarker(persistentTopic, pulsarTopic)
                .thenApply(ignored -> new PartitionLog(persistentTopic, formatter));
        });
    }

    private CompletableFuture<Topic> loadTopic(String pulsarTopic, boolean createIfMissing) {
        Map<String, String> properties = createIfMissing ? kafkaTopicProperties() : null;
        return brokerService.getTopic(pulsarTopic, createIfMissing, properties).thenCompose(topic -> topic
            .map(CompletableFuture::completedFuture)
            .orElseGet(() -> CompletableFuture.failedFuture(
                new UnknownTopicOrPartitionException(pulsarTopic))));
    }

    private CompletableFuture<Void> ensureKafkaTopicMarker(
            PersistentTopic persistentTopic, String pulsarTopic) {
        ManagedLedger ledger = persistentTopic.getManagedLedger();
        if (KAFKA_TOPIC_PROPERTY_VALUE.equals(ledger.getProperties().get(KAFKA_TOPIC_PROPERTY))) {
            return CompletableFuture.completedFuture(null);
        }
        if (ledger.getNumberOfEntries() == 0) {
            return CompletableFuture.failedFuture(new InvalidTopicException(
                "Unmarked empty Pulsar topic cannot be safely adopted as Kafka: " + pulsarTopic));
        }

        CompletableFuture<Void> result = new CompletableFuture<>();
        ledger.asyncReadEntry(ledger.getFirstPosition().getNext(), new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                boolean legacyKafkaTopic;
                try {
                    legacyKafkaTopic = KafkaEntryFormatter.isKafkaFormattedEntry(entry.getDataBuffer());
                } catch (RuntimeException error) {
                    result.completeExceptionally(error);
                    return;
                } finally {
                    entry.release();
                }
                if (!legacyKafkaTopic) {
                    result.completeExceptionally(new InvalidTopicException(
                        "Pulsar topic is not an isolated kpulse Kafka topic: " + pulsarTopic));
                    return;
                }
                migrateLegacyTopicMarker(ledger, result);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null);
        return result;
    }

    private static void migrateLegacyTopicMarker(
            ManagedLedger ledger, CompletableFuture<Void> result) {
        Map<String, String> properties = new HashMap<>(ledger.getProperties());
        properties.put(KAFKA_TOPIC_PROPERTY, KAFKA_TOPIC_PROPERTY_VALUE);
        ledger.asyncSetProperties(properties, new AsyncCallbacks.UpdatePropertiesCallback() {
            @Override
            public void updatePropertiesComplete(Map<String, String> updated, Object ctx) {
                result.complete(null);
            }

            @Override
            public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null);
    }
}
