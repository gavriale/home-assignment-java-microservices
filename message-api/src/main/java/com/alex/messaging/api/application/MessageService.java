package com.alex.messaging.api.application;

import com.alex.messaging.api.domain.exception.MessageNotFoundException;
import com.alex.messaging.event.CreateRequested;
import com.alex.messaging.event.DeleteRequested;
import com.alex.messaging.event.ReadReply;
import com.alex.messaging.event.ReadRequested;
import com.alex.messaging.event.UpdateRequested;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;

/**
 * Orchestrates the four use cases. Framework-free apart from {@code @Service}: no Kafka or HTTP
 * types appear here, so it is unit-testable with a fake {@link MessagePublisher} and no broker.
 */
@Service
public class MessageService {

    private static final Logger log = LoggerFactory.getLogger(MessageService.class);

    private final MessagePublisher publisher;
    private final EventIdGenerator eventIdGenerator;
    private final Clock clock;

    public MessageService(MessagePublisher publisher, EventIdGenerator eventIdGenerator, Clock clock) {
        this.publisher = publisher;
        this.eventIdGenerator = eventIdGenerator;
        this.clock = clock;
    }

    public MessageAccepted create(int messageId, String msg, String correlationId) {
        var event = new CreateRequested(eventIdGenerator.newEventId(), messageId, msg, clock.instant(), correlationId);
        var result = publisher.publishCreate(event);
        logPublished(messageId, result);
        return new MessageAccepted(event.eventId(), messageId, correlationId);
    }

    public MessageAccepted update(int messageId, String msg, String correlationId) {
        var event = new UpdateRequested(eventIdGenerator.newEventId(), messageId, msg, clock.instant(), correlationId);
        var result = publisher.publishUpdate(event);
        logPublished(messageId, result);
        return new MessageAccepted(event.eventId(), messageId, correlationId);
    }

    public MessageAccepted delete(int messageId, String correlationId) {
        var event = new DeleteRequested(eventIdGenerator.newEventId(), messageId, clock.instant(), correlationId);
        var result = publisher.publishDelete(event);
        logPublished(messageId, result);
        return new MessageAccepted(event.eventId(), messageId, correlationId);
    }

    public MessageView read(int messageId, String correlationId) {
        var event = new ReadRequested(eventIdGenerator.newEventId(), messageId, clock.instant(), correlationId);
        ReadReply reply = publisher.publishReadAndAwaitReply(event);
        String msg = reply.msg().orElseThrow(() -> new MessageNotFoundException(messageId));
        return new MessageView(messageId, msg);
    }

    private void logPublished(int messageId, PublishResult result) {
        log.info("published id={} topic={} partition={} offset={}",
                messageId, result.topic(), result.partition(), result.offset());
    }
}
