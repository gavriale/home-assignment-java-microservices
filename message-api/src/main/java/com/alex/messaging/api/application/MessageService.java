package com.alex.messaging.api.application;

import com.alex.messaging.api.domain.exception.MessageNotFoundException;
import com.alex.messaging.event.CreateRequested;
import com.alex.messaging.event.DeleteRequested;
import com.alex.messaging.event.ReadReply;
import com.alex.messaging.event.ReadRequested;
import com.alex.messaging.event.UpdateRequested;
import org.springframework.stereotype.Service;

import java.time.Clock;

/**
 * Orchestrates the four use cases. Framework-free apart from {@code @Service}: no Kafka or HTTP
 * types appear here, so it is unit-testable with a fake {@link MessagePublisher} and no broker.
 */
@Service
public class MessageService {

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
        publisher.publishCreate(event);
        return new MessageAccepted(event.eventId(), messageId, correlationId);
    }

    public MessageAccepted update(int messageId, String msg, String correlationId) {
        var event = new UpdateRequested(eventIdGenerator.newEventId(), messageId, msg, clock.instant(), correlationId);
        publisher.publishUpdate(event);
        return new MessageAccepted(event.eventId(), messageId, correlationId);
    }

    public MessageAccepted delete(int messageId, String correlationId) {
        var event = new DeleteRequested(eventIdGenerator.newEventId(), messageId, clock.instant(), correlationId);
        publisher.publishDelete(event);
        return new MessageAccepted(event.eventId(), messageId, correlationId);
    }

    public MessageView read(int messageId, String correlationId) {
        var event = new ReadRequested(eventIdGenerator.newEventId(), messageId, clock.instant(), correlationId);
        ReadReply reply = publisher.publishReadAndAwaitReply(event);
        String msg = reply.msg().orElseThrow(() -> new MessageNotFoundException(messageId));
        return new MessageView(messageId, msg);
    }
}
