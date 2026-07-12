package dev.kpulse.format;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

/**
 * The "kafka" entry format: a producer's {@link MemoryRecords} batch is stored verbatim as a single
 * Pulsar entry payload, wrapped in one Pulsar {@link MessageMetadata} tagged {@code entry.format=kafka}.
 *
 * <p>Encoding is a wrap (no re-serialization); decoding is a byte copy plus an 8-byte base-offset
 * patch per record batch. Patching each {@code baseOffset} (bytes 0-7) is safe because the v2 batch CRC
 * covers only bytes from {@code attributes} (offset 21) onward — the base offset is outside CRC
 * coverage, so rewriting it leaves the batch valid.
 */
public final class KafkaEntryFormatter {

    static final String IDENTITY_KEY = "entry.format";
    static final String IDENTITY_VALUE = "kafka";

    /** Wrap a Kafka batch as a Pulsar entry payload. {@code numMessages} drives the broker index increment. */
    public ByteBuf encode(MemoryRecords records, int numMessages) {
        return encode(records, numMessages, "kpulse", 0L);
    }

    /** Wrap records with the publish identity Pulsar deduplication uses. */
    public ByteBuf encode(MemoryRecords records, int numMessages, String producerName, long sequenceId) {
        MessageMetadata metadata = new MessageMetadata();
        metadata.addProperty().setKey(IDENTITY_KEY).setValue(IDENTITY_VALUE);
        metadata.setProducerName(producerName);
        metadata.setSequenceId(sequenceId);
        metadata.setHighestSequenceId(sequenceId + numMessages - 1L);
        metadata.setPublishTime(System.currentTimeMillis());
        metadata.setNumMessagesInBatch(numMessages);

        ByteBuf payload = Unpooled.wrappedBuffer(records.buffer());
        try {
            return Commands.serializeMetadataAndPayload(Commands.ChecksumType.None, metadata, payload);
        } finally {
            payload.release();
        }
    }

    /**
     * Rebuild the Kafka records for a Fetch response from stored entries, stamping each batch with its
     * real Kafka base offset. Every {@link Entry} is released before returning.
     *
     * <p>M1 requires v2 batches on ingest and modern clients request v2, so no down-conversion is
     * performed; the {@code magic} parameter is reserved for that later.
     */
    public MemoryRecords decode(List<Entry> entries, byte magic) {
        // Release every entry on all paths — a throw mid-loop (short/corrupt/foreign entry) must not
        // leak the entries not yet reached. Batch bytes are copied out before release, so this is safe.
        try {
            List<byte[]> batches = new ArrayList<>(entries.size());
            int totalSize = 0;
            for (Entry entry : entries) {
                long baseOffset = EntryMetadataUtils.peekBaseOffset(entry);
                ByteBuf buf = entry.getDataBuffer();
                MessageMetadata metadata = Commands.parseMessageMetadata(buf);
                if (!hasKafkaFormatMarker(metadata)) {
                    throw new InvalidRecordException(
                        "Pulsar topic contains an entry not written in kpulse Kafka format");
                }
                int numberOfMessages = metadata.getNumMessagesInBatch();

                byte[] batch = new byte[buf.readableBytes()];
                buf.getBytes(buf.readerIndex(), batch);
                patchBatchOffsets(batch, baseOffset, numberOfMessages);

                batches.add(batch);
                totalSize += batch.length;
            }

            ByteBuffer merged = ByteBuffer.allocate(totalSize);
            for (byte[] batch : batches) {
                merged.put(batch);
            }
            merged.flip();
            return MemoryRecords.readableRecords(merged);
        } finally {
            entries.forEach(Entry::release);
        }
    }

    private static void patchBatchOffsets(byte[] recordsBytes, long baseOffset, int numberOfMessages) {
        MemoryRecords records = MemoryRecords.readableRecords(ByteBuffer.wrap(recordsBytes));
        long nextOffset = baseOffset;
        for (MutableRecordBatch batch : records.batches()) {
            long batchSize = batch.lastOffset() - batch.baseOffset() + 1;
            if (batchSize < 1 || batchSize > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Invalid Kafka record-batch offset span: " + batchSize);
            }
            batch.setLastOffset(nextOffset + batchSize - 1);
            nextOffset += batchSize;
        }
        if (nextOffset - baseOffset != numberOfMessages) {
            throw new IllegalArgumentException(
                "Kafka record count does not match Pulsar batch metadata: records="
                    + (nextOffset - baseOffset) + ", metadata=" + numberOfMessages);
        }
    }

    private static boolean hasKafkaFormatMarker(MessageMetadata metadata) {
        return metadata.getPropertiesList().stream().anyMatch(property ->
            IDENTITY_KEY.equals(property.getKey()) && IDENTITY_VALUE.equals(property.getValue()));
    }

    /** Return whether an entry carries kpulse's Kafka payload marker without consuming the buffer. */
    public static boolean isKafkaFormattedEntry(ByteBuf entry) {
        MessageMetadata metadata = Commands.peekMessageMetadata(entry, "kpulse-format-check", -1L);
        return metadata != null && hasKafkaFormatMarker(metadata);
    }
}
