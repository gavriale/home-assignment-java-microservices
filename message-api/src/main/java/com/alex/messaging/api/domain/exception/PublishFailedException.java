package com.alex.messaging.api.domain.exception;

/**
 * The broker rejected or could not be reached for a publish attempt. The caller's request was
 * not accepted; nothing was queued for MS-2 to process.
 */
public class PublishFailedException extends MessagingException {

    public PublishFailedException(String topic, int messageId, Throwable cause) {
        super("Failed to publish to topic=%s messageId=%d".formatted(topic, messageId), cause);
    }
}
