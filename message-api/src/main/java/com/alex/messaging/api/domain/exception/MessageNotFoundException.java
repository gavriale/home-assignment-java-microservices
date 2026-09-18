package com.alex.messaging.api.domain.exception;

/** MS-2 reported no message exists for the requested id. */
public class MessageNotFoundException extends MessagingException {

    public MessageNotFoundException(int messageId) {
        super("No message found for id=%d".formatted(messageId));
    }
}
