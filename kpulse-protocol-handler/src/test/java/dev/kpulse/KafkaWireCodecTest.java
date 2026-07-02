package dev.kpulse;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.junit.jupiter.api.Test;

class KafkaWireCodecTest {

    @Test
    void decodesRequestHeaderAndBody() {
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, (short) 3, "kpulse-test", 42);
        ByteBuf frame = frame(new ApiVersionsRequest.Builder((short) 3).build((short) 3), header);

        KafkaHeaderAndRequest decoded = KafkaRequestDecoder.decode(frame);

        assertThat(decoded.header().apiKey()).isEqualTo(ApiKeys.API_VERSIONS);
        assertThat(decoded.header().apiVersion()).isEqualTo((short) 3);
        assertThat(decoded.header().correlationId()).isEqualTo(42);
        assertThat(decoded.request()).isInstanceOf(ApiVersionsRequest.class);
        decoded.release();
    }

    @Test
    void decodesMetadataRequestForNamedTopics() {
        RequestHeader header = new RequestHeader(ApiKeys.METADATA, (short) 12, "c", 7);
        MetadataRequest request = new MetadataRequest.Builder(java.util.List.of("orders"), false).build((short) 12);
        ByteBuf frame = frame(request, header);

        KafkaHeaderAndRequest decoded = KafkaRequestDecoder.decode(frame);

        assertThat(decoded.request()).isInstanceOf(MetadataRequest.class);
        assertThat(((MetadataRequest) decoded.request()).topics()).containsExactly("orders");
        decoded.release();
    }

    @Test
    void encodesResponsePreservingCorrelationId() {
        short version = 3;
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, version, "c", 99);

        ByteBuf encoded = KafkaResponseEncoder.encode(header, KafkaResponseFactory.apiVersions());

        ByteBuffer nio = encoded.nioBuffer();
        ResponseHeader responseHeader =
            ResponseHeader.parse(nio, ApiKeys.API_VERSIONS.responseHeaderVersion(version));
        assertThat(responseHeader.correlationId()).isEqualTo(99);

        ApiVersionsResponse response = ApiVersionsResponse.parse(new ByteBufferAccessor(nio), version);
        assertThat(response.data().errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(response.data().apiKeys()).hasSize(KafkaResponseFactory.ADVERTISED_APIS.size());
        encoded.release();
    }

    private static ByteBuf frame(org.apache.kafka.common.requests.AbstractRequest request, RequestHeader header) {
        return Unpooled.wrappedBuffer(request.serializeWithHeader(header));
    }
}
