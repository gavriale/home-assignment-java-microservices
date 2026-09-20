package com.alex.messaging.api.adapter.in.rest;

import com.alex.messaging.api.application.MessageAccepted;

import java.util.UUID;

/** REST-facing view of {@link MessageAccepted} — kept distinct so the application layer never
 * leaks directly into the wire response shape (DTO + Mapper). */
public record MessageAcceptedResponse(UUID eventId, int id, String correlationId) {

    public static MessageAcceptedResponse from(MessageAccepted accepted) {
        return new MessageAcceptedResponse(accepted.eventId(), accepted.messageId(), accepted.correlationId());
    }
}
