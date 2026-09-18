package com.alex.messaging.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Requests that the message identified by {@code messageId} be deleted. MS-2 treats this as
 * delete-if-exists so duplicate or out-of-order delivery is harmless — see ADR-004 and ADR-006.
 */
public record DeleteRequested(UUID eventId, int messageId, Instant occurredAt, String correlationId)
        implements WriteRequested {
}
