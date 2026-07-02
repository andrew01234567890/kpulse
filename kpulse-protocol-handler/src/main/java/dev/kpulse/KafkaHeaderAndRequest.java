package dev.kpulse;

import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;

/**
 * A decoded Kafka request plus the retained inbound frame.
 *
 * <p>For Produce, the request's {@code MemoryRecords} is a zero-copy slice of {@code frame}'s memory,
 * so the frame is retained on decode and must be {@link #release() released} only after the batch has
 * been serialized/persisted. Releasing early corrupts the persisted bytes.
 */
public final class KafkaHeaderAndRequest {

    private final RequestHeader header;
    private final AbstractRequest request;
    private final ByteBuf frame;

    public KafkaHeaderAndRequest(RequestHeader header, AbstractRequest request, ByteBuf frame) {
        this.header = header;
        this.request = request;
        this.frame = frame;
    }

    public RequestHeader header() {
        return header;
    }

    public AbstractRequest request() {
        return request;
    }

    public void release() {
        frame.release();
    }
}
