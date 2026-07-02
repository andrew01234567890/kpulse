package dev.kpulse.storage;

import org.apache.kafka.common.record.internal.MemoryRecords;

/** The outcome of a Fetch read: the records plus the offsets a Fetch response needs. */
public record ReadResult(MemoryRecords records, long highWatermark, long logStartOffset) {
}
