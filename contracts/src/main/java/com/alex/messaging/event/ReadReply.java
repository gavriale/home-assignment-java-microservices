package com.alex.messaging.event;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Reply payload MS-2 returns for a {@link ReadRequested} event. {@code msg} is empty when no
 * message exists for {@code messageId} — absence here is meaningful, so it is modelled with
 * {@link Optional} rather than a nullable field.
 */
public record ReadReply(UUID eventId, int messageId, Optional<String> msg, Instant occurredAt,
                         String correlationId) {
}
