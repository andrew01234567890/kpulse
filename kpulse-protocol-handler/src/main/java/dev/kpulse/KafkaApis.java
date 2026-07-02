package dev.kpulse;

import dev.kpulse.KafkaResponseFactory.TopicMetadata;
import dev.kpulse.storage.PartitionLog;
import dev.kpulse.storage.ReadResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.TopicPartition;
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
import org.apache.kafka.common.requests.FindCoordinatorRequest;
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

    private final KafkaRequestContext context;

    public KafkaApis(KafkaRequestContext context) {
        this.context = context;
    }

    public CompletableFuture<AbstractResponse> handle(KafkaHeaderAndRequest request) {
        ApiKeys apiKey = request.header().apiKey();
        return switch (apiKey) {
            case API_VERSIONS -> completed(KafkaResponseFactory.apiVersions());
            case METADATA -> handleMetadata(request);
            case PRODUCE -> handleProduce(request);
            case FETCH -> handleFetch(request);
            case FIND_COORDINATOR -> handleFindCoordinator(request);
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
        if (topics == null || topics.isEmpty()) {
            return completed(KafkaResponseFactory.metadata(version, context.selfNode(), context.clusterId(), List.of()));
        }

        List<CompletableFuture<TopicMetadata>> resolved = new ArrayList<>(topics.size());
        for (MetadataRequestData.MetadataRequestTopic topic : topics) {
            String name = topic.name();
            String pulsarTopic = context.topicMapper().toPulsarTopic(name);
            resolved.add(context.topicManager().getPartitionLog(pulsarTopic)
                .handle((log, error) -> new TopicMetadata(name,
                    error == null ? Errors.NONE : Errors.UNKNOWN_TOPIC_OR_PARTITION)));
        }
        return allOf(resolved).thenApply(topicMetadata ->
            KafkaResponseFactory.metadata(version, context.selfNode(), context.clusterId(), topicMetadata));
    }

    private CompletableFuture<AbstractResponse> handleProduce(KafkaHeaderAndRequest request) {
        ProduceRequest produceRequest = (ProduceRequest) request.request();
        List<CompletableFuture<ProduceResponseData.TopicProduceResponse>> topicResponses = new ArrayList<>();

        for (ProduceRequestData.TopicProduceData topicData : produceRequest.data().topicData()) {
            String pulsarTopic = context.topicMapper().toPulsarTopic(topicData.name());
            List<CompletableFuture<ProduceResponseData.PartitionProduceResponse>> partitionResponses =
                new ArrayList<>();
            for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {
                int partition = partitionData.index();
                MemoryRecords records = (MemoryRecords) partitionData.records();
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
        List<CompletableFuture<FetchResponseData.FetchableTopicResponse>> topicResponses = new ArrayList<>();

        fetchRequest.data().topics().forEach(topic -> {
            String pulsarTopic = context.topicMapper().toPulsarTopic(topic.topic());
            List<CompletableFuture<FetchResponseData.PartitionData>> partitionResponses = new ArrayList<>();
            topic.partitions().forEach(partition -> {
                int partitionIndex = partition.partition();
                long fetchOffset = partition.fetchOffset();
                int maxBytes = partition.partitionMaxBytes();
                partitionResponses.add(context.topicManager().getPartitionLog(pulsarTopic)
                    .thenCompose(log -> log.readRecords(fetchOffset, maxBytes, MAX_FETCH_ENTRIES))
                    .handle((result, error) -> error == null
                        ? KafkaResponseFactory.fetchPartition(partitionIndex, Errors.NONE,
                            result.highWatermark(), result.logStartOffset(), result.records())
                        : KafkaResponseFactory.fetchPartition(partitionIndex, Errors.forException(unwrap(error)),
                            -1L, -1L, MemoryRecords.EMPTY)));
            });
            topicResponses.add(allOf(partitionResponses).thenApply(partitions ->
                new FetchResponseData.FetchableTopicResponse().setTopic(topic.topic()).setPartitions(partitions)));
        });
        return allOf(topicResponses).thenApply(KafkaResponseFactory::fetch);
    }

    private CompletableFuture<AbstractResponse> handleFindCoordinator(KafkaHeaderAndRequest request) {
        FindCoordinatorRequest coordinatorRequest = (FindCoordinatorRequest) request.request();
        short version = request.header().apiVersion();
        List<String> keys = version >= FindCoordinatorRequest.MIN_BATCHED_VERSION
            ? coordinatorRequest.data().coordinatorKeys()
            : List.of(coordinatorRequest.data().key());
        return completed(KafkaResponseFactory.findCoordinator(version, keys, context.selfNode()));
    }

    private CompletableFuture<AbstractResponse> handleListOffsets(KafkaHeaderAndRequest request) {
        ListOffsetsRequest listRequest = (ListOffsetsRequest) request.request();
        List<CompletableFuture<ListOffsetsResponseData.ListOffsetsTopicResponse>> topicResponses = new ArrayList<>();

        for (ListOffsetsRequestData.ListOffsetsTopic topic : listRequest.data().topics()) {
            String pulsarTopic = context.topicMapper().toPulsarTopic(topic.name());
            for (ListOffsetsRequestData.ListOffsetsPartition partition : topic.partitions()) {
                TopicPartition topicPartition = new TopicPartition(topic.name(), partition.partitionIndex());
                long timestamp = partition.timestamp();
                topicResponses.add(context.topicManager().getPartitionLog(pulsarTopic)
                    .thenCompose(log -> offsetForTimestamp(log, timestamp))
                    .handle((offset, error) -> error == null
                        ? KafkaResponseFactory.listOffsetsTopic(topicPartition, Errors.NONE, timestamp, offset)
                        : KafkaResponseFactory.listOffsetsTopic(topicPartition,
                            Errors.forException(unwrap(error)), ListOffsetsResponse.UNKNOWN_TIMESTAMP,
                            ListOffsetsResponse.UNKNOWN_OFFSET)));
            }
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

    private static <T> CompletableFuture<List<T>> allOf(List<CompletableFuture<T>> futures) {
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
            .thenApply(ignored -> futures.stream().map(CompletableFuture::join).toList());
    }

    private static Throwable unwrap(Throwable error) {
        return (error instanceof java.util.concurrent.CompletionException && error.getCause() != null)
            ? error.getCause() : error;
    }
}
