package com.alex.messaging.processor.config;

import com.alex.messaging.processor.domain.exception.InvalidEventException;
import com.alex.messaging.topic.Topics;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Retryable exceptions (DB unavailable, transient I/O) get exponential backoff, capped, then
 * the dead letter topic. Non-retryable exceptions — deserialization failures (surfaced via
 * {@code ErrorHandlingDeserializer}) and our own {@link InvalidEventException} — skip retries
 * entirely and go straight to the DLT: retrying a poison message forever blocks the partition
 * (CLAUDE.md §5.6). All four operations share one dead letter topic rather than one per
 * source topic — simpler to operate, at the cost of per-operation DLT lag visibility.
 */
@Configuration
@EnableConfigurationProperties(RetryProperties.class)
public class KafkaErrorHandlingConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate, RetryProperties retryProperties) {
        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(Topics.DEAD_LETTER, -1));

        var backOff = new ExponentialBackOffWithMaxRetries(retryProperties.maxRetries());
        backOff.setInitialInterval(retryProperties.initialInterval().toMillis());
        backOff.setMultiplier(retryProperties.multiplier());
        backOff.setMaxInterval(retryProperties.maxInterval().toMillis());

        var errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(InvalidEventException.class);
        return errorHandler;
    }
}
