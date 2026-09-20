package com.alex.messaging.api.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * {@code partitionCount} must match the partition count MS-2 provisions for
 * {@code messages.read.reply.v1} — both sides hardcode the same default because
 * {@code contracts} deliberately carries no configuration.
 */
@ConfigurationProperties(prefix = "messaging.kafka.reply")
@Validated
public record ReplyProperties(@NotNull Duration timeout, @Min(1) int partitionCount) {
}
