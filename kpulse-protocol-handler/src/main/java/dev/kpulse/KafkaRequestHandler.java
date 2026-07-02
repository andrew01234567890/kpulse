package dev.kpulse;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-connection Kafka request handler: decode, dispatch to {@link KafkaApis}, and flush responses in
 * request order.
 *
 * <p>Produce and Fetch complete asynchronously and out of order, but Kafka requires per-connection
 * responses to be written in the order requests arrived so the client can match correlation IDs. Each
 * request reserves a queue slot on arrival; completed responses drain only from the head of the queue.
 * All queue access happens on the channel event loop, so no locking is needed.
 */
public class KafkaRequestHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger log = LoggerFactory.getLogger(KafkaRequestHandler.class);

    private final Function<KafkaHeaderAndRequest, CompletableFuture<AbstractResponse>> dispatch;
    private final ArrayDeque<PendingResponse> queue = new ArrayDeque<>();

    public KafkaRequestHandler(KafkaRequestContext context) {
        this(new KafkaApis(context)::handle);
    }

    KafkaRequestHandler(Function<KafkaHeaderAndRequest, CompletableFuture<AbstractResponse>> dispatch) {
        this.dispatch = dispatch;
    }

    private static final class PendingResponse {
        private final RequestHeader header;
        private AbstractResponse response;

        private PendingResponse(RequestHeader header) {
            this.header = header;
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf frame) {
        if (isUnsupportedApiVersions(frame)) {
            writeApiVersionsBootstrap(ctx, frame);
            return;
        }

        KafkaHeaderAndRequest request;
        try {
            request = KafkaRequestDecoder.decode(frame);
        } catch (RuntimeException e) {
            log.warn("Closing connection {}: undecodable Kafka request", ctx.channel().remoteAddress(), e);
            ctx.close();
            return;
        }

        if (!KafkaApis.expectsResponse(request)) {
            dispatch.apply(request).whenComplete((response, error) -> request.release());
            return;
        }

        PendingResponse slot = new PendingResponse(request.header());
        queue.addLast(slot);
        dispatch.apply(request).whenComplete((response, error) -> ctx.executor().execute(() -> {
            slot.response = (error == null)
                ? response
                : request.request().getErrorResponse(error);
            request.release();
            flushReady(ctx);
        }));
    }

    private void flushReady(ChannelHandlerContext ctx) {
        while (!queue.isEmpty() && queue.peekFirst().response != null) {
            PendingResponse done = queue.pollFirst();
            ctx.write(KafkaResponseEncoder.encode(done.header, done.response));
        }
        ctx.flush();
    }

    private static boolean isUnsupportedApiVersions(ByteBuf frame) {
        if (frame.readableBytes() < Integer.BYTES) {
            return false;
        }
        int readerIndex = frame.readerIndex();
        short apiKeyId = frame.getShort(readerIndex);
        short apiVersion = frame.getShort(readerIndex + Short.BYTES);
        return apiKeyId == ApiKeys.API_VERSIONS.id && !ApiKeys.API_VERSIONS.isVersionSupported(apiVersion);
    }

    private static void writeApiVersionsBootstrap(ChannelHandlerContext ctx, ByteBuf frame) {
        int correlationId = frame.getInt(frame.readerIndex() + 2 * Short.BYTES);
        ResponseHeader responseHeader =
            new ResponseHeader(correlationId, ApiKeys.API_VERSIONS.responseHeaderVersion((short) 0));
        ctx.writeAndFlush(KafkaResponseEncoder.encode(
            responseHeader, (short) 0, KafkaResponseFactory.apiVersionsUnsupported()));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Closing Kafka connection {} after error", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}
