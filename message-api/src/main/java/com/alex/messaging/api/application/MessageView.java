package com.alex.messaging.api.application;

/** Result of the Read use case. */
public record MessageView(int messageId, String msg) {
}
