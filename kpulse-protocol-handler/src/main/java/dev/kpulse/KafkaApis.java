package dev.kpulse;

import dev.kpulse.KafkaResponseFactory.TopicMetadata;
import dev.kpulse.storage.PartitionLog;
import dev.kpulse.storage.ReadResult;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidRequiredAcksException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.ProduceRequest;

/**
 * Dispatches decoded Kafka requests to per-API handlers, each returning a
 * {@code CompletableFuture<AbstractResponse>}. Holds no Netty state so it is unit-testable on its own.
 */
public final class KafkaApis {

    private static final int MAX_FETCH_ENTRIES = 1000;
    private static final int MAX_FETCH_PARTITIONS = 1000;
    private static final int MAX_FETCH_RESPONSE_BYTES = 50 * 1024 * 1024;

    private final KafkaRequestContext context;

    public KafkaApis(KafkaRequestContext context) {
        this.context = context;
    }

    public CompletableFuture<AbstractResponse> handle(KafkaHeaderAndRequest request) {
        ApiKeys apiKey = request.header().apiKey();
        short version = request.header().apiVersion();
        if (!KafkaResponseFactory.isVersionSupported(apiKey, version)) {
            return CompletableFuture.failedFuture(new UnsupportedVersionException(
                "kpulse does not support " + apiKey + " version " + version));
        }
        return switch (apiKey) {
            case API_VERSIONS -> completed(KafkaResponseFactory.apiVersions());
            case METADATA -> handleMetadata(request);
            case PRODUCE -> handleProduce(request);
            case FETCH -> handleFetch(request);
            case LIST_OFFSETS -> handleListOffsets(request);
            default -> CompletableFuture.failedFuture(
                new UnsupportedOperationException("kpulse M1 does not implement " + apiKey));
        };
    }

    /** A Produce with acks=0 is fire-and-forget: the client expects no response. */
    public static boolean expectsResponse(KafkaHeaderAndRequest request) {
        return !(request.request() instanceof ProduceRequest produce) || produce.acks() != 0;
    }

    private CompletableFuture<AbstractResponse> handleMetadata(KafkaHeaderAndRequest request) {
        MetadataRequest metadataRequest = (MetadataRequest) request.request();
        short version = request.header().apiVersion();
        List<MetadataRequestData.MetadataRequestTopic> topics = metadataRequest.data().topics();
        if (metadataRequest.isAllTopics()) {
            return context.listKafkaTopics().thenApply(names -> {
                List<TopicMetadata> metadata = names.stream()
                    .map(name -> new TopicMetadata(name, Errors.NONE))
                    .toList();
                return KafkaResponseFactory.metadata(
                    version, context.selfNode(), context.clusterId(), metadata);
            });
        }
        if (topics.isEmpty()) {
            return completed(KafkaResponseFactory.metadata(version, context.selfNode(), context.clusterId(), List.of()));
        }

        List<CompletableFuture<TopicMetadata>> resolved = new ArrayList<>(topics.size());
        for (MetadataRequestData.MetadataRequestTopic topic : topics) {
            String name = topic.name();
            String pulsarTopic = validPulsarTopic(name);
            if (pulsarTopic == null) {
                resolved.add(completed(new TopicMetadata(name, Errors.INVALID_TOPIC_EXCEPTION)));
                continue;
            }
            resolved.add(context.topicManager().getPartitionLog(
                    pulsarTopic, metadataRequest.data().allowAutoTopicCreation())
                .handle((log, error) -> new TopicMetadata(name,
                    error == null ? Errors.NONE : Errors.forException(unwrap(error)))));
        }
        return allOf(resolved).thenApply(topicMetadata ->
            KafkaResponseFactory.metadata(version, context.selfNode(), context.clusterId(), topicMetadata));
    }

    private CompletableFuture<AbstractResponse> handleProduce(KafkaHeaderAndRequest request) {
        ProduceRequest produceRequest = (ProduceRequest) request.request();
        if (produceRequest.acks() < -1 || produceRequest.acks() > 1) {
            return CompletableFuture.failedFuture(new InvalidRequiredAcksException(
                "acks must be -1, 0, or 1, got " + produceRequest.acks()));
        }
        List<CompletableFuture<ProduceResponseData.TopicProduceResponse>> topicResponses = new ArrayList<>();
        Map<ProduceRequestData.TopicProduceData, String> pulsarTopics = new IdentityHashMap<>();
        Map<ProduceRequestData.PartitionProduceData, MemoryRecords> copiedRecords = new IdentityHashMap<>();
        Map<ProduceRequestData.PartitionProduceData, Errors> validationErrors = new IdentityHashMap<>();
        for (ProduceRequestData.TopicProduceData topicData : produceRequest.data().topicData()) {
            pulsarTopics.put(topicData, validPulsarTopic(topicData.name()));
            for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {
                if (!isSupportedPartition(partitionData.index())) {
                    continue;
                }
                try {
                    MemoryRecords records = (MemoryRecords) partitionData.records();
                    PartitionLog.validateAndCount(records);
                    copiedRecords.put(partitionData, copyRecords(records));
                } catch (RuntimeException error) {
                    validationErrors.put(partitionData, Errors.forException(error));
                }
            }
        }

        for (ProduceRequestData.TopicProduceData topicData : produceRequest.data().topicData()) {
            String pulsarTopic = pulsarTopics.get(topicData);
            List<CompletableFuture<ProduceResponseData.PartitionProduceResponse>> partitionResponses =
                new ArrayList<>();
            for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {
                int partition = partitionData.index();
                if (pulsarTopic == null) {
                    partitionResponses.add(completed(KafkaResponseFactory.producePartition(
                        partition, Errors.INVALID_TOPIC_EXCEPTION, -1L)));
                    continue;
                }
                if (!isSupportedPartition(partition)) {
                    partitionResponses.add(completed(
                        KafkaResponseFactory.producePartition(
                            partition, Errors.UNKNOWN_TOPIC_OR_PARTITION, -1L)));
                    continue;
                }
                Errors validationError = validationErrors.get(partitionData);
                if (validationError != null) {
                    partitionResponses.add(completed(KafkaResponseFactory.producePartition(
                        partition, validationError, -1L)));
                    continue;
                }
                MemoryRecords records = copiedRecords.get(partitionData);
                partitionResponses.add(context.topicManager().getPartitionLog(pulsarTopic)
                    .thenCompose(log -> log.appendRecords(records))
                    .handle((baseOffset, error) -> error == null
                        ? KafkaResponseFactory.producePartition(partition, Errors.NONE, baseOffset)
                        : KafkaResponseFactory.producePartition(partition, Errors.forException(unwrap(error)), -1L)));
            }
            topicResponses.add(allOf(partitionResponses).thenApply(responses -> {
                ProduceResponseData.TopicProduceResponse topicResponse =
                    new ProduceResponseData.TopicProduceResponse().setName(topicData.name());
                responses.forEach(topicResponse.partitionResponses()::add);
                return topicResponse;
            }));
        }
        return allOf(topicResponses).thenApply(responses ->
            KafkaResponseFactory.produce(responses));
    }

    private CompletableFuture<AbstractResponse> handleFetch(KafkaHeaderAndRequest request) {
        FetchRequest fetchRequest = (FetchRequest) request.request();
        Set<RequestedPartition> requestedPartitions = new HashSet<>();
        for (org.apache.kafka.common.message.FetchRequestData.FetchTopic topic : fetchRequest.data().topics()) {
            for (org.apache.kafka.common.message.FetchRequestData.FetchPartition partition : topic.partitions()) {
                if (requestedPartitions.size() >= MAX_FETCH_PARTITIONS) {
                    return CompletableFuture.failedFuture(new InvalidRequestException(
                        "Fetch request exceeds the " + MAX_FETCH_PARTITIONS + " partition limit"));
                }
                if (!requestedPartitions.add(new RequestedPartition(topic.topic(), partition.partition()))) {
                    return CompletableFuture.failedFuture(new InvalidRequestException(
                        "Fetch request contains a duplicate topic-partition"));
                }
            }
        }
        List<FetchResponseData.FetchableTopicResponse> topicResponses = new ArrayList<>();
        FetchBudget budget = new FetchBudget(Math.max(0,
            Math.min(fetchRequest.maxBytes(), MAX_FETCH_RESPONSE_BYTES)));
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);

        for (org.apache.kafka.common.message.FetchRequestData.FetchTopic topic : fetchRequest.data().topics()) {
            String pulsarTopic = validPulsarTopic(topic.topic());
            FetchResponseData.FetchableTopicResponse topicResponse =
                new FetchResponseData.FetchableTopicResponse().setTopic(topic.topic());
            topicResponses.add(topicResponse);
            for (org.apache.kafka.common.message.FetchRequestData.FetchPartition partition : topic.partitions()) {
                int partitionIndex = partition.partition();
                chain = chain.thenCompose(ignored -> {
                    if (pulsarTopic == null) {
                        topicResponse.partitions().add(KafkaResponseFactory.fetchPartition(
                            partitionIndex, Errors.INVALID_TOPIC_EXCEPTION,
                            -1L, -1L, MemoryRecords.EMPTY));
                        return CompletableFuture.completedFuture(null);
                    }
                    if (!isSupportedPartition(partitionIndex)) {
                        topicResponse.partitions().add(KafkaResponseFactory.fetchPartition(
                        partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION,
                            -1L, -1L, MemoryRecords.EMPTY));
                        return CompletableFuture.completedFuture(null);
                    }
                    if (budget.remaining() == 0) {
                        // The response byte budget is exhausted. Do not resolve or read more topics:
                        // a malicious request must not turn a zero-byte response into storage work.
                        topicResponse.partitions().add(KafkaResponseFactory.fetchPartition(
                            partitionIndex, Errors.NONE, -1L, -1L, MemoryRecords.EMPTY));
                        return CompletableFuture.completedFuture(null);
                    }
                    int maxBytes = Math.max(0,
                        Math.min(partition.partitionMaxBytes(), budget.remaining()));
                    return context.topicManager().getPartitionLog(pulsarTopic, false)
                        .thenCompose(log -> log.readRecords(
                            partition.fetchOffset(), maxBytes, MAX_FETCH_ENTRIES))
                        .handle((result, error) -> {
                            FetchResponseData.PartitionData response;
                            if (error == null) {
                                budget.consume(result.records().sizeInBytes());
                                response = KafkaResponseFactory.fetchPartition(partitionIndex, Errors.NONE,
                                    result.highWatermark(), result.logStartOffset(), result.records());
                            } else {
                                response = KafkaResponseFactory.fetchPartition(
                                    partitionIndex, Errors.forException(unwrap(error)),
                                    -1L, -1L, MemoryRecords.EMPTY);
                            }
                            topicResponse.partitions().add(response);
                            return null;
                        });
                });
            }
        }
        return chain.thenApply(ignored -> KafkaResponseFactory.fetch(topicResponses));
    }

    private CompletableFuture<AbstractResponse> handleListOffsets(KafkaHeaderAndRequest request) {
        ListOffsetsRequest listRequest = (ListOffsetsRequest) request.request();
        List<CompletableFuture<ListOffsetsResponseData.ListOffsetsTopicResponse>> topicResponses = new ArrayList<>();

        for (ListOffsetsRequestData.ListOffsetsTopic topic : listRequest.data().topics()) {
            String pulsarTopic = validPulsarTopic(topic.name());
            List<CompletableFuture<ListOffsetsResponseData.ListOffsetsPartitionResponse>> partitionResponses =
                new ArrayList<>();
            for (ListOffsetsRequestData.ListOffsetsPartition partition : topic.partitions()) {
                int partitionIndex = partition.partitionIndex();
                long timestamp = partition.timestamp();
                if (pulsarTopic == null) {
                    partitionResponses.add(completed(KafkaResponseFactory.listOffsetsPartition(
                        partitionIndex, Errors.INVALID_TOPIC_EXCEPTION,
                        ListOffsetsResponse.UNKNOWN_TIMESTAMP, ListOffsetsResponse.UNKNOWN_OFFSET)));
                    continue;
                }
                if (!isSupportedPartition(partitionIndex)) {
                    partitionResponses.add(completed(KafkaResponseFactory.listOffsetsPartition(
                        partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION,
                        ListOffsetsResponse.UNKNOWN_TIMESTAMP, ListOffsetsResponse.UNKNOWN_OFFSET)));
                    continue;
                }
                partitionResponses.add(context.topicManager().getPartitionLog(pulsarTopic, false)
                    .thenCompose(log -> offsetForTimestamp(log, timestamp))
                    .handle((offset, error) -> error == null
                        ? KafkaResponseFactory.listOffsetsPartition(
                            partitionIndex, Errors.NONE, ListOffsetsResponse.UNKNOWN_TIMESTAMP, offset)
                        : KafkaResponseFactory.listOffsetsPartition(partitionIndex,
                            Errors.forException(unwrap(error)), ListOffsetsResponse.UNKNOWN_TIMESTAMP,
                            ListOffsetsResponse.UNKNOWN_OFFSET)));
            }
            topicResponses.add(allOf(partitionResponses).thenApply(partitions ->
                new ListOffsetsResponseData.ListOffsetsTopicResponse()
                    .setName(topic.name())
                    .setPartitions(partitions)));
        }
        return allOf(topicResponses).thenApply(KafkaResponseFactory::listOffsets);
    }

    private static CompletableFuture<Long> offsetForTimestamp(PartitionLog log, long timestamp) {
        if (timestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
            return log.earliestOffset();
        }
        if (timestamp == ListOffsetsRequest.LATEST_TIMESTAMP) {
            return CompletableFuture.completedFuture(log.logEndOffset());
        }
        // Timestamp-based lookup (OffsetFinder) is post-M1. Signal "no offset found" (-1) rather than
        // the log end, which would make offsetsForTimes seek to the tail and silently skip all records.
        return CompletableFuture.completedFuture(ListOffsetsResponse.UNKNOWN_OFFSET);
    }

    private static CompletableFuture<AbstractResponse> completed(AbstractResponse response) {
        return CompletableFuture.completedFuture(response);
    }

    private static <T> CompletableFuture<T> completed(T value) {
        return CompletableFuture.completedFuture(value);
    }

    private static boolean isSupportedPartition(int partition) {
        return partition == 0;
    }

    private String validPulsarTopic(String kafkaTopic) {
        try {
            return context.topicMapper().toPulsarTopic(kafkaTopic);
        } catch (IllegalArgumentException | NullPointerException error) {
            return null;
        }
    }

    private static final class FetchBudget {
        private int remaining;

        private FetchBudget(int remaining) {
            this.remaining = remaining;
        }

        private int remaining() {
            return remaining;
        }

        private void consume(int bytes) {
            remaining = Math.max(0, remaining - bytes);
        }
    }

    private record RequestedPartition(String topic, int partition) {
    }

    private static MemoryRecords copyRecords(MemoryRecords records) {
        ByteBuffer source = records.buffer().duplicate();
        ByteBuffer copy = ByteBuffer.allocate(source.remaining());
        copy.put(source).flip();
        return MemoryRecords.readableRecords(copy);
    }

    private static <T> CompletableFuture<List<T>> allOf(List<CompletableFuture<T>> futures) {
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
            .thenApply(ignored -> futures.stream().map(CompletableFuture::join).toList());
    }

    private static Throwable unwrap(Throwable error) {
        return (error instanceof java.util.concurrent.CompletionException && error.getCause() != null)
            ? error.getCause() : error;
    }
}
