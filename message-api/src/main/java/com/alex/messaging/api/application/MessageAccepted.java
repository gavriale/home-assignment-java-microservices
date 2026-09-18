package com.alex.messaging.api.application;

import java.util.UUID;

/** Result of a Create/Update/Delete use case: the request was published, not yet applied. */
public record MessageAccepted(UUID eventId, int messageId, String correlationId) {
}
