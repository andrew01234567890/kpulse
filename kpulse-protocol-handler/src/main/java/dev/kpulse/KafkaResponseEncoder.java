package dev.kpulse;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.ResponseHeader;

/**
 * Encodes a response for a given request header into a length-prefix-free {@link ByteBuf}; the
 * pipeline's {@code LengthFieldPrepender} adds the 4-byte frame length on write.
 */
public final class KafkaResponseEncoder {

    private KafkaResponseEncoder() {
    }

    public static ByteBuf encode(RequestHeader header, AbstractResponse response) {
        ResponseHeader responseHeader = header.toResponseHeader();
        return encode(responseHeader, header.apiVersion(), response);
    }

    /** Encode at an explicit body version — used for the ApiVersions bootstrap reply (always v0). */
    public static ByteBuf encode(ResponseHeader responseHeader, short apiVersion, AbstractResponse response) {
        ByteBuffer buffer = RequestUtils.serialize(
            responseHeader.data(), responseHeader.headerVersion(), response.data(), apiVersion);
        return Unpooled.wrappedBuffer(buffer);
    }
}
