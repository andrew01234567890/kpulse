package dev.kpulse;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * Per-connection Netty pipeline: Kafka's 4-byte length-prefixed framing feeding the request handler.
 */
public class KafkaChannelInitializer extends ChannelInitializer<SocketChannel> {

    // Generous ceiling on a single Kafka request; tightened to socket.request.max.bytes later.
    private static final int MAX_FRAME_LENGTH = 100 * 1024 * 1024;

    private final KafkaServiceConfiguration config;

    public KafkaChannelInitializer(KafkaServiceConfiguration config) {
        this.config = config;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ch.pipeline().addLast("frameDecoder",
            new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
        ch.pipeline().addLast("handler", new KafkaRequestHandler(config));
    }
}
