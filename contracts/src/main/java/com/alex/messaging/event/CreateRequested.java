package com.alex.messaging.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Requests that the message identified by {@code messageId} be created. MS-2 treats this as an
 * upsert so duplicate or out-of-order delivery is harmless — see ADR-004 and ADR-006.
 */
public record CreateRequested(UUID eventId, int messageId, String msg, Instant occurredAt, String correlationId)
        implements WriteRequested {
}
