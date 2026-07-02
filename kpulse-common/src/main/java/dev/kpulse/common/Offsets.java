package dev.kpulse.common;

/**
 * Pure offset arithmetic shared by the produce and read paths.
 *
 * <p>Kafka offsets are Pulsar's monotonic broker-entry index (stamped by
 * {@code AppendIndexMetadataInterceptor}), not derived from {@code (ledgerId, entryId)}. A batch of
 * {@code N} messages is stamped with the index of its <em>last</em> message, so the batch's base
 * offset is {@code index - (N - 1)}.
 */
public final class Offsets {

    private Offsets() {
    }

    /** Base (first) Kafka offset of a batch whose last message was stamped with {@code index}. */
    public static long baseOffset(long index, int numMessages) {
        if (numMessages < 1) {
            throw new IllegalArgumentException("numMessages must be >= 1, got " + numMessages);
        }
        return index - (numMessages - 1);
    }

    /**
     * Log end offset (the offset the next appended message will receive) for a topic whose current
     * broker-entry index is {@code index}. An empty log has {@code index == -1}, giving LEO 0.
     */
    public static long logEndOffset(long index) {
        return index + 1;
    }
}
