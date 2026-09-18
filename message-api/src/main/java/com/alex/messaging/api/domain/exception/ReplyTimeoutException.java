package com.alex.messaging.api.domain.exception;

/**
 * No reply arrived from MS-2 on {@code messages.read.reply.v1} within the configured timeout.
 * See ADR-003: Read is synchronous request-reply over an asynchronous transport, so this is a
 * real and expected outcome under broker or consumer-side pressure, not a bug.
 */
public class ReplyTimeoutException extends MessagingException {

    public ReplyTimeoutException(int messageId, Throwable cause) {
        super("Timed out waiting for read reply messageId=%d".formatted(messageId), cause);
    }
}
