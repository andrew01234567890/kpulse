package dev.kpulse.storage;

import dev.kpulse.format.EntryMetadataUtils;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.service.Topic;

/**
 * A {@link Topic.PublishContext} that recovers a published batch's Kafka base offset.
 *
 * <p>{@code publishMessage} is fire-and-forget, so the base offset is delivered asynchronously:
 * the broker calls {@link #setMetadataFromEntryData} with the stamped entry bytes, then
 * {@link #completed}, at which point the offset future resolves. Recycled to avoid per-batch
 * allocation on the produce hot path.
 */
public final class MessagePublishContext implements Topic.PublishContext {

    private static final Recycler<MessagePublishContext> RECYCLER = new Recycler<>() {
        @Override
        protected MessagePublishContext newObject(Handle<MessagePublishContext> handle) {
            return new MessagePublishContext(handle);
        }
    };

    private final Recycler.Handle<MessagePublishContext> handle;
    private CompletableFuture<Long> offsetFuture;
    private int numberOfMessages;
    private long baseOffset;
    private ByteBuf payload;
    private String producerName;
    private long sequenceId;

    private MessagePublishContext(Recycler.Handle<MessagePublishContext> handle) {
        this.handle = handle;
    }

    public static MessagePublishContext get(
            CompletableFuture<Long> offsetFuture, int numberOfMessages, ByteBuf payload,
            String producerName, long sequenceId) {
        MessagePublishContext context = RECYCLER.get();
        context.offsetFuture = offsetFuture;
        context.numberOfMessages = numberOfMessages;
        context.baseOffset = -1L;
        context.payload = payload;
        context.producerName = producerName;
        context.sequenceId = sequenceId;
        return context;
    }

    @Override
    public String getProducerName() {
        return producerName;
    }

    @Override
    public long getSequenceId() {
        return sequenceId;
    }

    @Override
    public long getHighestSequenceId() {
        return sequenceId + numberOfMessages - 1L;
    }

    @Override
    public long getNumberOfMessages() {
        return numberOfMessages;
    }

    @Override
    public void setMetadataFromEntryData(ByteBuf entryData) {
        this.baseOffset = EntryMetadataUtils.peekBaseOffset(entryData);
    }

    @Override
    public void completed(Exception exception, long ledgerId, long entryId) {
        CompletableFuture<Long> future = this.offsetFuture;
        long offset = this.baseOffset;
        ByteBuf toRelease = this.payload;
        recycle();
        if (toRelease != null) {
            toRelease.release();
        }
        if (exception != null) {
            future.completeExceptionally(exception);
        } else {
            future.complete(offset);
        }
    }

    private void recycle() {
        this.offsetFuture = null;
        this.numberOfMessages = 0;
        this.baseOffset = -1L;
        this.payload = null;
        this.producerName = null;
        this.sequenceId = -1L;
        handle.recycle(this);
    }
}
