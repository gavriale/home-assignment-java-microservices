package com.alex.messaging.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Requests the current value of the message identified by {@code messageId}. Answered
 * asynchronously with a {@link ReadReply} on {@link com.alex.messaging.topic.Topics#READ_REPLY}
 * — see ADR-003 for why Read uses request-reply instead of a simple fire-and-forget publish.
 */
public record ReadRequested(UUID eventId, int messageId, Instant occurredAt, String correlationId)
        implements MessageEvent {
}
