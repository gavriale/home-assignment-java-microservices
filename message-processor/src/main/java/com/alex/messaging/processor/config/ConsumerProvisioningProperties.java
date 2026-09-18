package com.alex.messaging.processor.config;

import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/** Concurrency and poll-batch size, shared by every per-operation listener container factory. */
@ConfigurationProperties(prefix = "messaging.kafka.consumer")
@Validated
public record ConsumerProvisioningProperties(@Min(1) int concurrency, @Min(1) int maxPollRecords) {
}
