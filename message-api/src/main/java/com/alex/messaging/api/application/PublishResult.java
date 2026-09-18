package com.alex.messaging.api.application;

/** Where a published record landed — logged at the boundary, never the intent to publish. */
public record PublishResult(String topic, int partition, long offset) {
}
