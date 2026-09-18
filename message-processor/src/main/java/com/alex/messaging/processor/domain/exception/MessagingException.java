package com.alex.messaging.processor.domain.exception;

/**
 * Root of the MS-2 domain exception hierarchy. Unchecked: transport concerns (Kafka, JPA)
 * must not leak checked-exception handling into the domain.
 */
public abstract class MessagingException extends RuntimeException {

    protected MessagingException(String message) {
        super(message);
    }
}
