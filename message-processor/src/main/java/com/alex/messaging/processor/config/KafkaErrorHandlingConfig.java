package com.alex.messaging.processor.config;

import com.alex.messaging.processor.domain.exception.InvalidEventException;
import com.alex.messaging.topic.Topics;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.NonTransientDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Retryable exceptions (DB unavailable, transient I/O) get exponential backoff, capped, then
 * the dead letter topic. Non-retryable exceptions skip retries entirely and go straight to the
 * DLT: retrying a poison message forever blocks the partition. Three kinds are registered as
 * non-retryable: deserialization failures (surfaced via {@code ErrorHandlingDeserializer}),
 * our own {@link InvalidEventException}, and Spring's {@link NonTransientDataAccessException} —
 * the parent of every "this will never succeed no matter how many times you retry" database
 * error (constraint violations, bad SQL, etc.), as opposed to
 * {@code TransientDataAccessException}, which covers errors a retry could plausibly fix (the DB
 * was briefly unreachable). {@code EventValidator} rejects an oversized {@code msg} before it
 * ever reaches the database; this is the safety net for whatever it doesn't anticipate.
 * All four operations share one dead letter topic rather than one per
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
        errorHandler.addNotRetryableExceptions(InvalidEventException.class, NonTransientDataAccessException.class);
        return errorHandler;
    }
}
