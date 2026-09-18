package com.alex.messaging.api.domain.exception;

/**
 * Root of the MS-1 domain exception hierarchy. Unchecked: transport concerns (Kafka, HTTP)
 * must not leak checked-exception handling into the domain.
 */
public abstract class MessagingException extends RuntimeException {

    protected MessagingException(String message) {
        super(message);
    }

    protected MessagingException(String message, Throwable cause) {
        super(message, cause);
    }
}
