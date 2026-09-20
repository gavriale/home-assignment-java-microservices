package com.alex.messaging.processor.domain.exception;

/**
 * An event failed validation on consume. Never trust the wire — {@code
 * contracts} carries no Jakarta annotations by design (ADR-002), so MS-2 validates by hand.
 * Non-retryable: a poison message that fails validation will fail identically on every retry,
 * so it goes straight to the dead letter topic.
 */
public class InvalidEventException extends MessagingException {

    public InvalidEventException(String message) {
        super(message);
    }
}
