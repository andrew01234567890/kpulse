package dev.kpulse;

import java.util.List;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceResponse;

/**
 * Builds Kafka response objects for the six APIs kpulse M1 implements. The advertised ApiVersions
 * surface is exactly these six keys: Metadata and Fetch are capped below their topic-ID versions
 * (v13+), since M1 resolves topics by name.
 */
public final class KafkaResponseFactory {

    /** An advertised API and the version range kpulse actually parses and serializes. */
    public record ApiRange(ApiKeys apiKey, short minVersion, short maxVersion) {
    }

    /** A resolved topic for a Metadata response: its Kafka name and lookup outcome. */
    public record TopicMetadata(String name, Errors error) {
    }

    public static final List<ApiRange> ADVERTISED_APIS = List.of(
        new ApiRange(ApiKeys.PRODUCE, (short) 3, (short) 12),
        new ApiRange(ApiKeys.FETCH, (short) 4, (short) 12),
        new ApiRange(ApiKeys.LIST_OFFSETS, (short) 1, (short) 7),
        new ApiRange(ApiKeys.METADATA, (short) 0, (short) 12),
        new ApiRange(ApiKeys.FIND_COORDINATOR, (short) 0, (short) 4),
        new ApiRange(ApiKeys.API_VERSIONS, (short) 0, (short) 4));

    private KafkaResponseFactory() {
    }

    public static ApiVersionsResponse apiVersions() {
        ApiVersionsResponseData data = new ApiVersionsResponseData()
            .setErrorCode(Errors.NONE.code())
            .setThrottleTimeMs(0);
        for (ApiRange range : ADVERTISED_APIS) {
            data.apiKeys().add(new ApiVersionsResponseData.ApiVersion()
                .setApiKey(range.apiKey().id)
                .setMinVersion(range.minVersion())
                .setMaxVersion(range.maxVersion()));
        }
        return new ApiVersionsResponse(data);
    }

    public static ApiVersionsResponse apiVersionsUnsupported() {
        return new ApiVersionsResponse(
            new ApiVersionsResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code()));
    }

    public static MetadataResponse metadata(short version, Node self, String clusterId, List<TopicMetadata> topics) {
        MetadataResponseData data = new MetadataResponseData()
            .setClusterId(clusterId)
            .setControllerId(self.id());
        data.brokers().add(new MetadataResponseData.MetadataResponseBroker()
            .setNodeId(self.id())
            .setHost(self.host())
            .setPort(self.port())
            .setRack(null));
        for (TopicMetadata topic : topics) {
            MetadataResponseData.MetadataResponseTopic responseTopic =
                new MetadataResponseData.MetadataResponseTopic()
                    .setName(topic.name())
                    .setErrorCode(topic.error().code())
                    .setIsInternal(false);
            if (topic.error() == Errors.NONE) {
                responseTopic.partitions().add(new MetadataResponseData.MetadataResponsePartition()
                    .setPartitionIndex(0)
                    .setErrorCode(Errors.NONE.code())
                    .setLeaderId(self.id())
                    .setLeaderEpoch(0)
                    .setReplicaNodes(List.of(self.id()))
                    .setIsrNodes(List.of(self.id()))
                    .setOfflineReplicas(List.of()));
            }
            data.topics().add(responseTopic);
        }
        return new MetadataResponse(data, version);
    }

    public static FindCoordinatorResponse findCoordinator(short version, List<String> keys, Node self) {
        if (version >= 4) {
            List<FindCoordinatorResponseData.Coordinator> coordinators = keys.stream()
                .map(key -> FindCoordinatorResponse.prepareCoordinatorResponse(Errors.NONE, key, self))
                .toList();
            return new FindCoordinatorResponse(new FindCoordinatorResponseData().setCoordinators(coordinators));
        }
        return FindCoordinatorResponse.prepareOldResponse(Errors.NONE, self);
    }

    public static ListOffsetsResponse listOffsets(List<ListOffsetsResponseData.ListOffsetsTopicResponse> topics) {
        return new ListOffsetsResponse(new ListOffsetsResponseData().setTopics(topics));
    }

    public static ListOffsetsResponseData.ListOffsetsTopicResponse listOffsetsTopic(
            TopicPartition partition, Errors error, long timestamp, long offset) {
        return ListOffsetsResponse.singletonListOffsetsTopicResponse(
            partition, error, timestamp, offset, ListOffsetsResponse.UNKNOWN_EPOCH);
    }

    public static ProduceResponse produce(List<ProduceResponseData.TopicProduceResponse> topics) {
        ProduceResponseData data = new ProduceResponseData();
        topics.forEach(data.responses()::add);
        return new ProduceResponse(data);
    }

    public static ProduceResponseData.PartitionProduceResponse producePartition(int partition, Errors error, long baseOffset) {
        return new ProduceResponseData.PartitionProduceResponse()
            .setIndex(partition)
            .setErrorCode(error.code())
            .setBaseOffset(baseOffset)
            .setLogAppendTimeMs(-1L)
            .setLogStartOffset(-1L);
    }

    public static FetchResponse fetch(List<FetchResponseData.FetchableTopicResponse> responses) {
        return FetchResponse.of(new FetchResponseData()
            .setThrottleTimeMs(0)
            .setErrorCode(Errors.NONE.code())
            .setResponses(responses));
    }

    public static FetchResponseData.PartitionData fetchPartition(int partition, Errors error,
            long highWatermark, long logStartOffset, MemoryRecords records) {
        return new FetchResponseData.PartitionData()
            .setPartitionIndex(partition)
            .setErrorCode(error.code())
            .setHighWatermark(highWatermark)
            .setLastStableOffset(highWatermark)
            .setLogStartOffset(logStartOffset)
            .setRecords(records);
    }
}
