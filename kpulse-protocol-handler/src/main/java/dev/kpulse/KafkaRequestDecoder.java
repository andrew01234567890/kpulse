package dev.kpulse;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;

/**
 * Decodes a de-framed Kafka request (no length prefix) into a {@link KafkaHeaderAndRequest}.
 *
 * <p>The inbound frame is retained because a Produce request's records are a zero-copy slice of it.
 */
public final class KafkaRequestDecoder {

    private KafkaRequestDecoder() {
    }

    public static KafkaHeaderAndRequest decode(ByteBuf frame) {
        ByteBuffer nio = frame.nioBuffer();
        RequestHeader header = RequestHeader.parse(nio);
        ApiKeys apiKey = header.apiKey();
        AbstractRequest request =
            AbstractRequest.parseRequest(apiKey, header.apiVersion(), new ByteBufferAccessor(nio)).request;
        return new KafkaHeaderAndRequest(header, request, frame.retain());
    }
}
