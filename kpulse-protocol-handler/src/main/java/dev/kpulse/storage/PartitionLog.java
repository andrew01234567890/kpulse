package dev.kpulse.storage;

import dev.kpulse.format.EntryMetadataUtils;
import dev.kpulse.format.KafkaEntryFormatter;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single Kafka topic-partition backed by one Pulsar topic. Owns the produce (append) and fetch
 * (read) paths; Kafka offsets are the Pulsar broker-entry index.
 */
public final class PartitionLog {

    private static final Logger log = LoggerFactory.getLogger(PartitionLog.class);

    private final PersistentTopic persistentTopic;
    private final KafkaEntryFormatter formatter;

    public PartitionLog(PersistentTopic persistentTopic, KafkaEntryFormatter formatter) {
        this.persistentTopic = persistentTopic;
        this.formatter = formatter;
    }

    /** Persist a Kafka batch, resolving to its assigned base (first) offset. */
    public CompletableFuture<Long> appendRecords(MemoryRecords records) {
        int numberOfMessages;
        ByteBuf entry;
        try {
            numberOfMessages = validateAndCount(records);
            entry = formatter.encode(records, numberOfMessages);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        CompletableFuture<Long> offsetFuture = new CompletableFuture<>();
        persistentTopic.publishMessage(entry, MessagePublishContext.get(offsetFuture, numberOfMessages));
        return offsetFuture;
    }

    /** Read records starting at (or just before) {@code fetchOffset}, up to the given limits. */
    public CompletableFuture<ReadResult> readRecords(long fetchOffset, int maxBytes, int maxEntries) {
        ManagedLedger ledger = persistentTopic.getManagedLedger();
        long highWatermark = EntryMetadataUtils.logEndOffset(ledger);
        if (fetchOffset >= highWatermark) {
            return CompletableFuture.completedFuture(new ReadResult(MemoryRecords.EMPTY, highWatermark, 0L));
        }

        CompletableFuture<ReadResult> result = new CompletableFuture<>();
        ledger.asyncFindPosition(entry -> offsetBelow(entry, fetchOffset)).whenComplete((position, findError) -> {
            if (findError != null) {
                result.completeExceptionally(findError);
                return;
            }
            Position start = (position == null) ? PositionFactory.EARLIEST : ledger.getPreviousPosition(position);
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
                        result.complete(new ReadResult(records, highWatermark, 0L));
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

    private static int validateAndCount(MemoryRecords records) {
        int numberOfMessages = 0;
        for (MutableRecordBatch batch : records.batches()) {
            if (batch.magic() != RecordBatch.MAGIC_VALUE_V2) {
                throw new InvalidRecordException(
                    "kpulse M1 requires record batch magic v2, got " + batch.magic());
            }
            batch.ensureValid();
            numberOfMessages += (int) (batch.lastOffset() - batch.baseOffset() + 1);
        }
        if (numberOfMessages == 0) {
            throw new InvalidRecordException("Produce request contained no records");
        }
        return numberOfMessages;
    }
}
