package com.alex.messaging.processor.config;

import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/** Partition/replication settings for the topics MS-2 owns provisioning for (reply, dead-letter). */
@ConfigurationProperties(prefix = "messaging.kafka.topics")
@Validated
public record TopicProvisioningProperties(@Min(1) int partitionCount, @Min(1) short replicationFactor) {
}
