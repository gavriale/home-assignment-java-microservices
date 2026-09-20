package com.alex.messaging.processor.config;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/** Exponential backoff for retryable consumer errors, capped, then dead-lettered. */
@ConfigurationProperties(prefix = "messaging.kafka.retry")
@Validated
public record RetryProperties(@Min(0) int maxRetries, @NotNull Duration initialInterval,
                               @DecimalMin("1.0") double multiplier, @NotNull Duration maxInterval) {
}
