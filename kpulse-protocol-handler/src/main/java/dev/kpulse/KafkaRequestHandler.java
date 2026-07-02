package dev.kpulse;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.nio.ByteBuffer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-connection Kafka request handler.
 *
 * <p>M0 decodes the request header and establishes the dispatch seam; per-API handling and response
 * encoding (in request order, as the Kafka protocol requires) are added incrementally from M1.
 */
public class KafkaRequestHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger log = LoggerFactory.getLogger(KafkaRequestHandler.class);

    @SuppressWarnings("unused") // used by per-API handlers from M1 onward
    private final KafkaServiceConfiguration config;

    public KafkaRequestHandler(KafkaServiceConfiguration config) {
        this.config = config;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf frame) {
        ByteBuffer buffer = frame.nioBuffer();
        RequestHeader header = RequestHeader.parse(buffer);
        ApiKeys apiKey = header.apiKey();
        if (log.isDebugEnabled()) {
            log.debug("Received {} v{} correlationId={} from {}",
                apiKey, header.apiVersion(), header.correlationId(), ctx.channel().remoteAddress());
        }
        dispatch(ctx, header);
    }

    private void dispatch(ChannelHandlerContext ctx, RequestHeader header) {
        // Placeholder until M1 wires real per-API handlers. Closing is safer than a malformed reply.
        log.warn("kpulse M0 does not yet answer {} — closing connection {}",
            header.apiKey(), ctx.channel().remoteAddress());
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Closing Kafka connection {} after error", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}
