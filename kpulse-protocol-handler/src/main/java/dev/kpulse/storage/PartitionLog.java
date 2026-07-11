package dev.kpulse.storage;

import dev.kpulse.format.EntryMetadataUtils;
import dev.kpulse.format.KafkaEntryFormatter;
import io.netty.buffer.ByteBuf;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.DefaultRecord;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single Kafka topic-partition backed by one Pulsar topic. Owns the produce (append) and fetch
 * (read) paths; Kafka offsets are the Pulsar broker-entry index.
 */
public final class PartitionLog {

    private static final Logger log = LoggerFactory.getLogger(PartitionLog.class);
    static final int MAX_RECORDS_BYTES = 50 * 1024 * 1024;
    private static final int MAX_RECORDS_PER_BATCH = 1_000_000;

    private final PersistentTopic persistentTopic;
    private final KafkaEntryFormatter formatter;
    private final String publishProducerName = "kpulse-" + UUID.randomUUID();
    private final AtomicLong nextSequenceId = new AtomicLong();

    public PartitionLog(PersistentTopic persistentTopic, KafkaEntryFormatter formatter) {
        this.persistentTopic = persistentTopic;
        this.formatter = formatter;
    }

    /** Persist a Kafka batch, resolving to its assigned base (first) offset. */
    public synchronized CompletableFuture<Long> appendRecords(MemoryRecords records) {
        int numberOfMessages;
        long sequenceId;
        ByteBuf entry;
        try {
            numberOfMessages = validateAndCount(records);
            sequenceId = nextSequenceId.getAndAdd(numberOfMessages);
            entry = formatter.encode(records, numberOfMessages, publishProducerName, sequenceId);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        CompletableFuture<Long> offsetFuture = new CompletableFuture<>();
        // The context releases `entry` in completed(): publishMessage retains its own duplicate, so
        // kpulse's ref must be dropped once the publish resolves, else the pooled buffer leaks.
        MessagePublishContext publishContext = MessagePublishContext.get(
            offsetFuture, numberOfMessages, entry, publishProducerName, sequenceId);
        try {
            persistentTopic.publishMessage(entry, publishContext);
        } catch (RuntimeException error) {
            publishContext.completed(error, -1L, -1L);
        }
        return offsetFuture;
    }

    /** Read records starting at (or just before) {@code fetchOffset}, up to the given limits. */
    public CompletableFuture<ReadResult> readRecords(long fetchOffset, int maxBytes, int maxEntries) {
        ManagedLedger ledger = persistentTopic.getManagedLedger();
        long highWatermark = EntryMetadataUtils.logEndOffset(ledger);
        if (fetchOffset > highWatermark) {
            return outOfRange(fetchOffset, highWatermark);
        }

        return earliestOffset().thenCompose(logStartOffset -> {
            if (fetchOffset < logStartOffset) {
                return outOfRange(fetchOffset, highWatermark);
            }
            if (fetchOffset == highWatermark || maxBytes <= 0 || maxEntries <= 0) {
                return CompletableFuture.completedFuture(
                    new ReadResult(MemoryRecords.EMPTY, highWatermark, logStartOffset));
            }
            return readRecordsFrom(fetchOffset, maxBytes, maxEntries, highWatermark, logStartOffset);
        });
    }

    private CompletableFuture<ReadResult> readRecordsFrom(
            long fetchOffset, int maxBytes, int maxEntries, long highWatermark, long logStartOffset) {
        ManagedLedger ledger = persistentTopic.getManagedLedger();
        CompletableFuture<ReadResult> result = new CompletableFuture<>();
        ledger.asyncFindPosition(entry -> offsetBelow(entry, fetchOffset)).whenComplete((position, findError) -> {
            if (findError != null) {
                result.completeExceptionally(findError);
                return;
            }
            if (position == null) {
                result.completeExceptionally(new ManagedLedgerException(
                    "Pulsar failed to locate the requested Kafka offset " + fetchOffset));
                return;
            }
            Position start = ledger.getPreviousPosition(position);
            ManagedCursor cursor;
            try {
                cursor = ledger.newNonDurableCursor(start, "kpulse-fetch-" + UUID.randomUUID());
            } catch (ManagedLedgerException e) {
                result.completeExceptionally(e);
                return;
            }
            cursor.asyncReadEntries(maxEntries, maxBytes, new ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    try {
                        MemoryRecords records = formatter.decode(entries, RecordBatch.CURRENT_MAGIC_VALUE);
                        result.complete(new ReadResult(records, highWatermark, logStartOffset));
                    } catch (RuntimeException e) {
                        result.completeExceptionally(e);
                    } finally {
                        deleteCursor(ledger, cursor);
                    }
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    deleteCursor(ledger, cursor);
                    result.completeExceptionally(exception);
                }
            }, null, PositionFactory.LATEST);
        });
        return result;
    }

    private static CompletableFuture<ReadResult> outOfRange(long fetchOffset, long highWatermark) {
        return CompletableFuture.failedFuture(new OffsetOutOfRangeException(
            "Fetch offset " + fetchOffset + " is outside the readable range ending at " + highWatermark));
    }

    /** Offset the next appended message will receive; also the high watermark. */
    public long logEndOffset() {
        return EntryMetadataUtils.logEndOffset(persistentTopic.getManagedLedger());
    }

    /** Earliest readable offset: the base offset of the first stored entry, or LEO for an empty log. */
    public CompletableFuture<Long> earliestOffset() {
        ManagedLedger ledger = persistentTopic.getManagedLedger();
        long logEndOffset = EntryMetadataUtils.logEndOffset(ledger);
        if (ledger.getNumberOfEntries() == 0) {
            return CompletableFuture.completedFuture(logEndOffset);
        }
        CompletableFuture<Long> result = new CompletableFuture<>();
        ManagedCursor cursor;
        try {
            cursor = ledger.newNonDurableCursor(PositionFactory.EARLIEST, "kpulse-earliest-" + UUID.randomUUID());
        } catch (ManagedLedgerException e) {
            return CompletableFuture.failedFuture(e);
        }
        cursor.asyncReadEntries(1, Long.MAX_VALUE, new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                try {
                    result.complete(entries.isEmpty() ? logEndOffset : EntryMetadataUtils.peekBaseOffset(entries.get(0)));
                } catch (RuntimeException e) {
                    result.completeExceptionally(e);
                } finally {
                    entries.forEach(Entry::release);
                    deleteCursor(ledger, cursor);
                }
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                deleteCursor(ledger, cursor);
                result.completeExceptionally(exception);
            }
        }, null, PositionFactory.LATEST);
        return result;
    }

    private static boolean offsetBelow(Entry entry, long fetchOffset) {
        try {
            return EntryMetadataUtils.peekIndex(entry.getDataBuffer()) < fetchOffset;
        } finally {
            entry.release();
        }
    }

    private static void deleteCursor(ManagedLedger ledger, ManagedCursor cursor) {
        ledger.asyncDeleteCursor(cursor.getName(), new org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback() {
            @Override
            public void deleteCursorComplete(Object ctx) {
            }

            @Override
            public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                log.warn("Failed to delete fetch cursor {}", cursor.getName(), exception);
            }
        }, null);
    }

    public static int validateAndCount(MemoryRecords records) {
        return validateAndCount(records, MAX_RECORDS_BYTES);
    }

    static int validateAndCount(MemoryRecords records, int maxDecompressedBytes) {
        if (records.sizeInBytes() > MAX_RECORDS_BYTES) {
            throw new InvalidRecordException(
                "Kafka records exceed the " + MAX_RECORDS_BYTES + " byte M1 limit");
        }
        if (records.validBytes() != records.sizeInBytes()) {
            throw new InvalidRecordException(
                "Kafka records contain trailing bytes or a partial record batch");
        }
        long numberOfMessages = 0L;
        long decompressedBytes = 0L;
        for (MutableRecordBatch batch : records.batches()) {
            if (batch.magic() != RecordBatch.MAGIC_VALUE_V2) {
                throw new InvalidRecordException(
                    "kpulse M1 requires record batch magic v2, got " + batch.magic());
            }
            batch.ensureValid();
            if (batch.isControlBatch() || batch.isTransactional() || batch.hasProducerId()) {
                throw new InvalidRecordException(
                    "kpulse M1 does not support control, transactional, or idempotent record batches");
            }
            long batchMessages;
            try {
                batchMessages = Math.addExact(
                    Math.subtractExact(batch.lastOffset(), batch.baseOffset()), 1L);
            } catch (ArithmeticException error) {
                throw new InvalidRecordException("Record-batch offset span overflows", error);
            }
            if (batchMessages < 1 || batchMessages > Integer.MAX_VALUE) {
                throw new InvalidRecordException("Invalid record-batch offset span: " + batchMessages);
            }
            Integer declaredCount = batch.countOrNull();
            if (declaredCount == null || declaredCount < 1
                    || declaredCount > MAX_RECORDS_PER_BATCH || declaredCount != batchMessages) {
                throw new InvalidRecordException(
                    "Record-batch count does not match its offset span");
            }
            int actualCount = 0;
            long expectedOffset = batch.baseOffset();
            if (!(batch instanceof DefaultRecordBatch defaultBatch)) {
                throw new InvalidRecordException("Unsupported Kafka record-batch implementation");
            }
            long remainingBytes = maxDecompressedBytes - decompressedBytes;
            Long logAppendTime = batch.timestampType() == TimestampType.LOG_APPEND_TIME
                ? batch.maxTimestamp() : null;
            try (InputStream decompressed = defaultBatch.recordInputStream(BufferSupplier.NO_CACHING);
                 LimitedInputStream bounded = new LimitedInputStream(decompressed, remainingBytes)) {
                for (; actualCount < declaredCount; actualCount++) {
                    long recordStart = bounded.bytesRead();
                    Record record = DefaultRecord.readPartiallyFrom(
                        bounded, batch.baseOffset(), defaultBatch.baseTimestamp(), batch.baseSequence(), logAppendTime);
                    long consumed = bounded.bytesRead() - recordStart;
                    if (record.sizeInBytes() < 1 || consumed != record.sizeInBytes()) {
                        throw new InvalidRecordException("Record size does not match its declared length");
                    }
                    if (record.offset() != expectedOffset) {
                        throw new InvalidRecordException(
                            "Record offsets must be contiguous within a batch");
                    }
                    expectedOffset++;
                }
                if (bounded.read() != -1) {
                    throw new InvalidRecordException(
                        "Record-batch count does not match the records in its payload");
                }
                decompressedBytes += bounded.bytesRead();
            } catch (IOException error) {
                throw new InvalidRecordException("Invalid or oversized compressed Kafka records", error);
            }
            if (actualCount != declaredCount) {
                throw new InvalidRecordException(
                    "Record-batch count does not match the records in its payload");
            }
            numberOfMessages += batchMessages;
            if (numberOfMessages > Integer.MAX_VALUE) {
                throw new InvalidRecordException("Produce request contains too many records");
            }
        }
        if (numberOfMessages == 0) {
            throw new InvalidRecordException("Produce request contained no records");
        }
        return (int) numberOfMessages;
    }

    /** Stops decompression after the request-wide byte budget, including skip-heavy records. */
    private static final class LimitedInputStream extends FilterInputStream {
        private final long limit;
        private long bytesRead;

        private LimitedInputStream(InputStream delegate, long limit) {
            super(delegate);
            this.limit = Math.max(0L, limit);
        }

        private long bytesRead() {
            return bytesRead;
        }

        @Override
        public int read() throws IOException {
            int value = super.read();
            if (value >= 0) {
                recordBytes(1L);
            }
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            int boundedLength = (int) Math.min(length, Math.max(1L, limit - bytesRead + 1L));
            int count = super.read(bytes, offset, boundedLength);
            if (count > 0) {
                recordBytes(count);
            }
            return count;
        }

        @Override
        public long skip(long count) throws IOException {
            long boundedCount = Math.min(count, Math.max(1L, limit - bytesRead + 1L));
            long skipped = super.skip(boundedCount);
            if (skipped > 0) {
                recordBytes(skipped);
            }
            return skipped;
        }

        private void recordBytes(long count) throws IOException {
            bytesRead += count;
            if (bytesRead > limit) {
                throw new IOException("Decompressed Kafka records exceed the M1 size limit");
            }
        }
    }
}
