package dev.kpulse;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;

class KafkaRequestHandlerOrderingTest {

    @Test
    void flushesResponsesInRequestOrderDespiteOutOfOrderCompletion() {
        Map<Integer, CompletableFuture<AbstractResponse>> pending = new ConcurrentHashMap<>();
        Function<KafkaHeaderAndRequest, CompletableFuture<AbstractResponse>> dispatch = request -> {
            CompletableFuture<AbstractResponse> future = new CompletableFuture<>();
            pending.put(request.header().correlationId(), future);
            return future;
        };
        EmbeddedChannel channel = new EmbeddedChannel(new KafkaRequestHandler(dispatch));

        channel.writeInbound(apiVersionsFrame(1), apiVersionsFrame(2), apiVersionsFrame(3));

        pending.get(2).complete(KafkaResponseFactory.apiVersions());
        pending.get(3).complete(KafkaResponseFactory.apiVersions());
        channel.runPendingTasks();
        assertThat(correlationIdsWritten(channel)).isEmpty();

        pending.get(1).complete(KafkaResponseFactory.apiVersions());
        channel.runPendingTasks();
        assertThat(correlationIdsWritten(channel)).containsExactly(1, 2, 3);

        channel.finishAndReleaseAll();
    }

    private static ByteBuf apiVersionsFrame(int correlationId) {
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, (short) 3, "c", correlationId);
        return Unpooled.wrappedBuffer(
            new ApiVersionsRequest.Builder((short) 3).build((short) 3).serializeWithHeader(header));
    }

    private static List<Integer> correlationIdsWritten(EmbeddedChannel channel) {
        List<Integer> correlationIds = new ArrayList<>();
        ByteBuf out;
        while ((out = channel.readOutbound()) != null) {
            correlationIds.add(out.getInt(out.readerIndex()));
            out.release();
        }
        return correlationIds;
    }
}
