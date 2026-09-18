package com.alex.messaging.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Wire-format contract for every operation MS-1 publishes to Kafka and MS-2 consumes. Each
 * operation is a distinct record so MS-2 can dispatch with an exhaustive {@code switch}: adding
 * a fifth operation without a matching handler fails the build instead of falling through.
 */
public sealed interface MessageEvent
        permits WriteRequested, ReadRequested {

    UUID eventId();

    int messageId();

    Instant occurredAt();

    String correlationId();
}
