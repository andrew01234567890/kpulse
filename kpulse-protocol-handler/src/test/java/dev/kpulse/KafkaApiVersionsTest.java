package dev.kpulse;

import static org.assertj.core.api.Assertions.assertThat;

import dev.kpulse.KafkaResponseFactory.ApiRange;
import java.util.stream.Collectors;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

class KafkaApiVersionsTest {

    @Test
    void advertisesExactlyTheSixImplementedApisWithTheirRanges() {
        assertThat(KafkaResponseFactory.ADVERTISED_APIS).containsExactly(
            new ApiRange(ApiKeys.PRODUCE, (short) 3, (short) 12),
            new ApiRange(ApiKeys.FETCH, (short) 4, (short) 12),
            new ApiRange(ApiKeys.LIST_OFFSETS, (short) 1, (short) 7),
            new ApiRange(ApiKeys.METADATA, (short) 0, (short) 12),
            new ApiRange(ApiKeys.FIND_COORDINATOR, (short) 0, (short) 4),
            new ApiRange(ApiKeys.API_VERSIONS, (short) 0, (short) 4));
    }

    @Test
    void metadataAndFetchAreCappedBelowTheirTopicIdVersions() {
        assertThat(rangeFor(ApiKeys.METADATA).maxVersion()).isLessThan(ApiKeys.METADATA.latestVersion());
        assertThat(rangeFor(ApiKeys.FETCH).maxVersion()).isLessThan(ApiKeys.FETCH.latestVersion());
    }

    @Test
    void apiVersionsResponseListsAllAdvertisedApisWithNoError() {
        var response = KafkaResponseFactory.apiVersions();
        assertThat(response.data().errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(response.data().apiKeys().stream().map(ApiVersion::apiKey).collect(Collectors.toSet()))
            .containsExactlyInAnyOrder((short) 0, (short) 1, (short) 2, (short) 3, (short) 10, (short) 18);
    }

    private static ApiRange rangeFor(ApiKeys apiKey) {
        return KafkaResponseFactory.ADVERTISED_APIS.stream()
            .filter(range -> range.apiKey() == apiKey)
            .findFirst()
            .orElseThrow();
    }
}
