package dev.kpulse;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

/**
 * Per-connection Netty pipeline: Kafka's 4-byte length-prefixed framing (stripped inbound, prepended
 * outbound) feeding the request handler, so the handler never touches the length field itself.
 */
public class KafkaChannelInitializer extends ChannelInitializer<SocketChannel> {

    // Generous ceiling on a single Kafka request; tightened to socket.request.max.bytes later.
    private static final int MAX_FRAME_LENGTH = 100 * 1024 * 1024;

    private final KafkaRequestContext context;

    public KafkaChannelInitializer(KafkaRequestContext context) {
        this.context = context;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ch.pipeline().addLast("frameDecoder",
            new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
        ch.pipeline().addLast("frameEncoder", new LengthFieldPrepender(4));
        ch.pipeline().addLast("handler", new KafkaRequestHandler(context));
    }
}
