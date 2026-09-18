package com.alex.messaging.processor.adapter.in.kafka;

import com.alex.messaging.event.ReadReply;
import com.alex.messaging.event.ReadRequested;
import com.alex.messaging.header.Headers;
import com.alex.messaging.processor.application.EventValidator;
import com.alex.messaging.processor.application.MessageRepositoryPort;
import com.alex.messaging.processor.domain.model.Message;
import com.alex.messaging.topic.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.Optional;

/**
 * Read is structurally different from the write operations — it produces a reply instead of
 * mutating state — so it is not part of the {@code OperationHandler} Strategy; it is its own
 * listener that returns the reply value directly (CLAUDE.md §5.3).
 */
@Component
public class ReadOperationListener {

    private static final Logger log = LoggerFactory.getLogger(ReadOperationListener.class);

    private final MessageRepositoryPort repository;
    private final Clock clock;
    private final EventValidator eventValidator;

    public ReadOperationListener(MessageRepositoryPort repository, Clock clock, EventValidator eventValidator) {
        this.repository = repository;
        this.clock = clock;
        this.eventValidator = eventValidator;
    }

    @KafkaListener(topics = Topics.READ, containerFactory = "readListenerContainerFactory")
    @SendTo
    public ReadReply onRead(ReadRequested event,
                             @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                             @Header(KafkaHeaders.OFFSET) long offset) {
        MDC.put(Headers.CORRELATION_ID, event.correlationId());
        try {
            log.info("consumed id={} partition={} offset={}", event.messageId(), partition, offset);
            eventValidator.validate(event.messageId());
            Optional<String> msg = repository.findById(event.messageId()).map(Message::msg);
            log.info("applied read id={} found={}", event.messageId(), msg.isPresent());
            return new ReadReply(event.eventId(), event.messageId(), msg, clock.instant(), event.correlationId());
        } finally {
            MDC.remove(Headers.CORRELATION_ID);
        }
    }
}
