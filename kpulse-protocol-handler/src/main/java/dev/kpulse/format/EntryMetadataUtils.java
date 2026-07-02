package dev.kpulse.format;

import dev.kpulse.common.Offsets;
import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.pulsar.broker.intercept.ManagedLedgerInterceptorImpl;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

/**
 * Reads Kafka offsets back out of Pulsar entries and managed ledgers.
 *
 * <p>Kafka offsets live in a separate monotonic counter maintained by
 * {@code AppendIndexMetadataInterceptor} ({@link BrokerEntryMetadata#getIndex()}), not in Pulsar's
 * {@code (ledgerId, entryId)} position. Every append is stamped with the index of its <em>last</em>
 * message, so a batch's base offset is {@code index - (numMessagesInBatch - 1)}.
 */
public final class EntryMetadataUtils {

    private EntryMetadataUtils() {
    }

    /** The broker-entry index stamped on this entry (offset of the batch's last message). */
    public static long peekIndex(ByteBuf headersAndPayload) {
        BrokerEntryMetadata metadata = Commands.peekBrokerEntryMetadataIfExist(headersAndPayload);
        if (metadata == null) {
            throw new IllegalStateException(
                "Entry has no BrokerEntryMetadata — is AppendIndexMetadataInterceptor enabled?");
        }
        return metadata.getIndex();
    }

    /** Base (first) Kafka offset of the batch stored in this entry. */
    public static long peekBaseOffset(ByteBuf headersAndPayload) {
        MessageMetadata metadata = Commands.peekMessageMetadata(headersAndPayload, null, 0);
        if (metadata == null) {
            throw new IllegalStateException("Entry has no MessageMetadata");
        }
        return Offsets.baseOffset(peekIndex(headersAndPayload), metadata.getNumMessagesInBatch());
    }

    public static long peekBaseOffset(Entry entry) {
        return peekBaseOffset(entry.getDataBuffer());
    }

    /** Current broker-entry index for the topic; {@code -1} before the first append. */
    public static long currentIndex(ManagedLedger ledger) {
        ManagedLedgerInterceptor interceptor = ledger.getManagedLedgerInterceptor();
        if (!(interceptor instanceof ManagedLedgerInterceptorImpl impl)) {
            return -1L;
        }
        return impl.getIndex();
    }

    /** Offset the next appended message will receive (also the high watermark). */
    public static long logEndOffset(ManagedLedger ledger) {
        return Offsets.logEndOffset(currentIndex(ledger));
    }
}
