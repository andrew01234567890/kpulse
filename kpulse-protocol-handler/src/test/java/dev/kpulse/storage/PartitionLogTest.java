package dev.kpulse.storage;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPOutputStream;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteUtils;
import org.junit.jupiter.api.Test;

class PartitionLogTest {

    @Test
    void rejectsOffsetSpanThatDoesNotMatchTheActualRecords() {
        MemoryRecordsBuilder builder = MemoryRecords.builder(
            ByteBuffer.allocate(1024), Compression.NONE, TimestampType.CREATE_TIME, 0L);
        builder.appendWithOffset(1_000_000L, new SimpleRecord("one".getBytes()));
        MemoryRecords records = builder.build();

        assertThatThrownBy(() -> PartitionLog.validateAndCount(records))
            .isInstanceOf(InvalidRecordException.class)
            .hasMessageContaining("count does not match");
    }

    @Test
    void rejectsProducerStateThatM1CannotHonor() {
        MemoryRecords records = MemoryRecords.withIdempotentRecords(
            Compression.NONE, 42L, (short) 0, 0, new SimpleRecord("one".getBytes()));

        assertThatThrownBy(() -> PartitionLog.validateAndCount(records))
            .isInstanceOf(InvalidRecordException.class)
            .hasMessageContaining("does not support");
    }

    @Test
    void rejectsHugeCompressedRecordDeclarationWithoutMaterializingIt() throws Exception {
        ByteArrayOutputStream rawRecord = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(rawRecord)) {
            ByteUtils.writeVarint(Integer.MAX_VALUE, output);
        }
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(compressed)) {
            gzip.write(rawRecord.toByteArray());
        }

        byte[] compressedRecord = compressed.toByteArray();
        int batchSize = DefaultRecordBatch.RECORD_BATCH_OVERHEAD + compressedRecord.length;
        ByteBuffer batch = ByteBuffer.allocate(batchSize);
        batch.position(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
        batch.put(compressedRecord);
        batch.position(0);
        DefaultRecordBatch.writeHeader(
            batch, 0L, 0, batchSize, RecordBatch.MAGIC_VALUE_V2, CompressionType.GZIP,
            TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false, false, 0, 1);
        batch.position(0);

        MemoryRecords records = MemoryRecords.readableRecords(batch);
        assertThatThrownBy(() -> PartitionLog.validateAndCount(records))
            .isInstanceOf(InvalidRecordException.class);
    }
}
