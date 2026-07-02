package dev.kpulse.format;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Records;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

/**
 * The "kafka" entry format: a producer's {@link MemoryRecords} batch is stored verbatim as a single
 * Pulsar entry payload, wrapped in one Pulsar {@link MessageMetadata} tagged {@code entry.format=kafka}.
 *
 * <p>Encoding is a wrap (no re-serialization); decoding is a byte copy plus an 8-byte base-offset
 * patch. Patching the batch's {@code baseOffset} (bytes 0-7) is safe because the v2 record-batch CRC
 * covers only bytes from {@code attributes} (offset 21) onward — the base offset is outside CRC
 * coverage, so rewriting it leaves the batch valid.
 */
public final class KafkaEntryFormatter {

    static final String IDENTITY_KEY = "entry.format";
    static final String IDENTITY_VALUE = "kafka";

    /** Wrap a Kafka batch as a Pulsar entry payload. {@code numMessages} drives the broker index increment. */
    public ByteBuf encode(MemoryRecords records, int numMessages) {
        MessageMetadata metadata = new MessageMetadata();
        metadata.addProperty().setKey(IDENTITY_KEY).setValue(IDENTITY_VALUE);
        metadata.setProducerName("");
        metadata.setSequenceId(0L);
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
                Commands.parseMessageMetadata(buf);

                byte[] batch = new byte[buf.readableBytes()];
                buf.getBytes(buf.readerIndex(), batch);
                ByteBuffer.wrap(batch).putLong(Records.OFFSET_OFFSET, baseOffset);

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
}
