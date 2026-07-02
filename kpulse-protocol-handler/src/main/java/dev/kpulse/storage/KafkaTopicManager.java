package dev.kpulse.storage;

import dev.kpulse.format.KafkaEntryFormatter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

/** Resolves and caches one {@link PartitionLog} per Pulsar topic. */
public final class KafkaTopicManager {

    private final BrokerService brokerService;
    private final KafkaEntryFormatter formatter;
    private final ConcurrentHashMap<String, CompletableFuture<PartitionLog>> logs = new ConcurrentHashMap<>();

    public KafkaTopicManager(BrokerService brokerService, KafkaEntryFormatter formatter) {
        this.brokerService = brokerService;
        this.formatter = formatter;
    }

    public CompletableFuture<PartitionLog> getPartitionLog(String pulsarTopic) {
        CompletableFuture<PartitionLog> future = logs.computeIfAbsent(pulsarTopic, this::createPartitionLog);
        future.exceptionally(error -> {
            logs.remove(pulsarTopic, future);
            return null;
        });
        return future;
    }

    private CompletableFuture<PartitionLog> createPartitionLog(String pulsarTopic) {
        return brokerService.getOrCreateTopic(pulsarTopic).thenApply(topic -> {
            if (!(topic instanceof PersistentTopic persistentTopic)) {
                throw new IllegalStateException("kpulse supports only persistent topics, got: " + pulsarTopic);
            }
            return new PartitionLog(persistentTopic, formatter);
        });
    }
}
