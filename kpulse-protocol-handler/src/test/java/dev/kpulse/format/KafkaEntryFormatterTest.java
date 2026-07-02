package dev.kpulse.format;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataInterceptor;
import org.apache.pulsar.common.protocol.Commands;
import org.junit.jupiter.api.Test;

class KafkaEntryFormatterTest {

    private final KafkaEntryFormatter formatter = new KafkaEntryFormatter();

    @Test
    void roundTripsRecordsAndStampsSequentialOffsetsAcrossBatches() {
        // One shared interceptor mirrors the broker stamping successive appends: index -1 -> 2 -> 4.
        Set<BrokerEntryMetadataInterceptor> interceptors = Set.of(new AppendIndexMetadataInterceptor());
        Entry first = stampedEntry(interceptors, records("a", "b", "c"), 3);
        Entry second = stampedEntry(interceptors, records("d", "e"), 2);

        MemoryRecords decoded = formatter.decode(mutableList(first, second), RecordBatch.MAGIC_VALUE_V2);

        List<Long> offsets = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (Record record : decoded.records()) {
            offsets.add(record.offset());
            values.add(utf8(record.value()));
        }
        assertThat(offsets).containsExactly(0L, 1L, 2L, 3L, 4L);
        assertThat(values).containsExactly("a", "b", "c", "d", "e");
    }

    @Test
    void patchingBaseOffsetLeavesTheBatchCrcValid() {
        Set<BrokerEntryMetadataInterceptor> interceptors = Set.of(new AppendIndexMetadataInterceptor());
        Entry entry = stampedEntry(interceptors, records("x", "y"), 2);

        MemoryRecords decoded = formatter.decode(mutableList(entry), RecordBatch.MAGIC_VALUE_V2);

        for (MutableRecordBatch batch : decoded.batches()) {
            assertThat(batch.baseOffset()).isZero();
            batch.ensureValid();
        }
    }

    private Entry stampedEntry(Set<BrokerEntryMetadataInterceptor> interceptors, MemoryRecords records,
            int numberOfMessages) {
        ByteBuf encoded = formatter.encode(records, numberOfMessages);
        return new TestEntry(Commands.addBrokerEntryMetadata(encoded, interceptors, numberOfMessages));
    }

    private static MemoryRecords records(String... values) {
        SimpleRecord[] simpleRecords = Arrays.stream(values)
            .map(value -> new SimpleRecord(value.getBytes(UTF_8)))
            .toArray(SimpleRecord[]::new);
        return MemoryRecords.withRecords(Compression.NONE, simpleRecords);
    }

    private static String utf8(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, UTF_8);
    }

    private static List<Entry> mutableList(Entry... entries) {
        return new ArrayList<>(Arrays.asList(entries));
    }

    private static final class TestEntry implements Entry {
        private final ByteBuf buffer;

        private TestEntry(ByteBuf buffer) {
            this.buffer = buffer;
        }

        @Override
        public byte[] getData() {
            byte[] bytes = new byte[buffer.readableBytes()];
            buffer.getBytes(buffer.readerIndex(), bytes);
            return bytes;
        }

        @Override
        public byte[] getDataAndRelease() {
            byte[] data = getData();
            release();
            return data;
        }

        @Override
        public int getLength() {
            return buffer.readableBytes();
        }

        @Override
        public ByteBuf getDataBuffer() {
            return buffer;
        }

        @Override
        public Position getPosition() {
            return PositionFactory.create(0, 0);
        }

        @Override
        public long getLedgerId() {
            return 0;
        }

        @Override
        public long getEntryId() {
            return 0;
        }

        @Override
        public boolean release() {
            return buffer.release();
        }
    }
}
