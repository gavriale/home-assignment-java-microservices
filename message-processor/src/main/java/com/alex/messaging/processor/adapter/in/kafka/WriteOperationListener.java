package com.alex.messaging.processor.adapter.in.kafka;

import com.alex.messaging.event.CreateRequested;
import com.alex.messaging.event.DeleteRequested;
import com.alex.messaging.event.UpdateRequested;
import com.alex.messaging.event.WriteRequested;
import com.alex.messaging.header.Headers;
import com.alex.messaging.processor.application.EventValidator;
import com.alex.messaging.processor.application.OperationHandlerRegistry;
import com.alex.messaging.topic.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * One listener method per operation topic, each bound to its own consumer-group container
 * factory. The correlation id is restored from the Kafka header into MDC so
 * every log line for this record ties back to the originating HTTP request.
 */
@Component
public class WriteOperationListener {

    private static final Logger log = LoggerFactory.getLogger(WriteOperationListener.class);

    private final OperationHandlerRegistry registry;
    private final EventValidator eventValidator;

    public WriteOperationListener(OperationHandlerRegistry registry, EventValidator eventValidator) {
        this.registry = registry;
        this.eventValidator = eventValidator;
    }

    @KafkaListener(topics = Topics.CREATE, containerFactory = "createListenerContainerFactory")
    public void onCreate(CreateRequested event,
                          @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                          @Header(KafkaHeaders.OFFSET) long offset) {
        process(event, partition, offset);
    }

    @KafkaListener(topics = Topics.UPDATE, containerFactory = "updateListenerContainerFactory")
    public void onUpdate(UpdateRequested event,
                          @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                          @Header(KafkaHeaders.OFFSET) long offset) {
        process(event, partition, offset);
    }

    @KafkaListener(topics = Topics.DELETE, containerFactory = "deleteListenerContainerFactory")
    public void onDelete(DeleteRequested event,
                          @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                          @Header(KafkaHeaders.OFFSET) long offset) {
        process(event, partition, offset);
    }

    private void process(WriteRequested event, int partition, long offset) {
        MDC.put(Headers.CORRELATION_ID, event.correlationId());
        try {
            log.info("consumed id={} partition={} offset={}", event.messageId(), partition, offset);
            eventValidator.validate(event);
            registry.dispatch(event);
        } finally {
            MDC.remove(Headers.CORRELATION_ID);
        }
    }
}
