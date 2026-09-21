package com.alex.messaging.validation;

/**
 * Limits both services must agree on: message-api's request validation, message-processor's
 * event validation, and the {@code messages} table's column size. One number, read from here
 * everywhere, so changing the limit can never leave one of the three checks out of sync.
 */
public final class MessageLimits {

    public static final int MAX_MSG_LENGTH = 1000;

    private MessageLimits() {
    }
}
