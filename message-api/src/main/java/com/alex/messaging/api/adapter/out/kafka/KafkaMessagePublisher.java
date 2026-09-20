package com.alex.messaging.api.adapter.out.kafka;

import com.alex.messaging.api.application.MessagePublisher;
import com.alex.messaging.api.domain.exception.PublishFailedException;
import com.alex.messaging.api.domain.exception.ReplyTimeoutException;
import com.alex.messaging.event.CreateRequested;
import com.alex.messaging.event.DeleteRequested;
import com.alex.messaging.event.MessageEvent;
import com.alex.messaging.event.ReadReply;
import com.alex.messaging.event.ReadRequested;
import com.alex.messaging.event.UpdateRequested;
import com.alex.messaging.header.Headers;
import com.alex.messaging.topic.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.KafkaReplyTimeoutException;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

/**
 * Publishes every event keyed by {@code messageId} as a String — same key, same partition,
 * ordered delivery per entity. Blocks briefly on the send future so the
 * caller can log the actual partition/offset the record landed on, not just the intent to
 * publish; cheap on the virtual-thread-per-request web layer this runs behind. The
 * correlation id is carried both in the event body and as a Kafka header, so MS-2 (and any
 * DLT tooling) can read it without deserializing the payload.
 */
@Component
public class KafkaMessagePublisher implements MessagePublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaMessagePublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ReplyingKafkaTemplate<String, Object, ReadReply> replyingKafkaTemplate;
    private final ReplyPartitionResolver replyPartitionResolver;

    public KafkaMessagePublisher(KafkaTemplate<String, Object> kafkaTemplate,
                                  ReplyingKafkaTemplate<String, Object, ReadReply> replyingKafkaTemplate,
                                  ReplyPartitionResolver replyPartitionResolver) {
        this.kafkaTemplate = kafkaTemplate;
        this.replyingKafkaTemplate = replyingKafkaTemplate;
        this.replyPartitionResolver = replyPartitionResolver;
    }

    @Override
    public void publishCreate(CreateRequested event) {
        send(Topics.CREATE, event);
    }

    @Override
    public void publishUpdate(UpdateRequested event) {
        send(Topics.UPDATE, event);
    }

    @Override
    public void publishDelete(DeleteRequested event) {
        send(Topics.DELETE, event);
    }

    @Override
    public ReadReply publishReadAndAwaitReply(ReadRequested event) {
        var record = new ProducerRecord<String, Object>(Topics.READ, String.valueOf(event.messageId()), event);
        record.headers().add(Headers.CORRELATION_ID, event.correlationId().getBytes(StandardCharsets.UTF_8));
        record.headers().add(KafkaHeaders.REPLY_TOPIC, Topics.READ_REPLY.getBytes(StandardCharsets.UTF_8));
        record.headers().add(KafkaHeaders.REPLY_PARTITION,
                ByteBuffer.allocate(4).putInt(replyPartitionResolver.partition()).array());

        RequestReplyFuture<String, Object, ReadReply> future = replyingKafkaTemplate.sendAndReceive(record);
        try {
            ConsumerRecord<String, ReadReply> response = future.get();
            logPublished(event.messageId(), future.getSendFuture().get());
            return response.value();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublishFailedException(Topics.READ, event.messageId(), e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof KafkaReplyTimeoutException timeout) {
                throw new ReplyTimeoutException(event.messageId(), timeout);
            }
            throw new PublishFailedException(Topics.READ, event.messageId(), e.getCause());
        }
    }

    /**
     * The one place every operation's "published id=... topic=... partition=... offset=..."
     * line is logged — {@link #send} and {@link #publishReadAndAwaitReply} both call this with
     * their own {@link SendResult} rather than threading publish metadata back up through
     * {@code MessageService}, which never needed it for anything but this log line.
     */
    private void logPublished(int messageId, SendResult<String, Object> result) {
        var metadata = result.getRecordMetadata();
        log.info("published id={} topic={} partition={} offset={}",
                messageId, metadata.topic(), metadata.partition(), metadata.offset());
    }

    private void send(String topic, MessageEvent event) {
        var record = new ProducerRecord<String, Object>(topic, String.valueOf(event.messageId()), event);
        record.headers().add(Headers.CORRELATION_ID, event.correlationId().getBytes(StandardCharsets.UTF_8));
        try {
            SendResult<String, Object> result = kafkaTemplate.send(record).get();
            logPublished(event.messageId(), result);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublishFailedException(topic, event.messageId(), e);
        } catch (ExecutionException e) {
            throw new PublishFailedException(topic, event.messageId(), e.getCause());
        }
    }
}
