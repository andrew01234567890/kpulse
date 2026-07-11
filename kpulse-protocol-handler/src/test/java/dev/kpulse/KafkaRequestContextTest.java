package dev.kpulse;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.pulsar.common.naming.TopicName;
import org.junit.jupiter.api.Test;

class KafkaRequestContextTest {

    @Test
    void discoversTenThousandImmediatelyCompletedTopicsWithoutRecursing() {
        List<TopicName> topics = IntStream.range(0, 10_000)
            .mapToObj(index -> TopicName.get("persistent://public/default/topic-" + index))
            .toList();

        List<String> discovered = KafkaRequestContext.inspectKafkaTopics(
            topics, 16, ignored -> CompletableFuture.completedFuture(true)).join();

        assertThat(discovered).hasSize(10_000);
    }
}
