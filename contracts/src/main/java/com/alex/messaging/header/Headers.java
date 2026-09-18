package com.alex.messaging.header;

/**
 * Kafka header names shared by MS-1 and MS-2, in addition to Spring Kafka's built-in headers
 * (e.g. {@code KafkaHeaders.CORRELATION_ID}, used internally by {@code ReplyingKafkaTemplate}).
 */
public final class Headers {

    /** Application-level correlation id propagated through MDC and Kafka headers for tracing. */
    public static final String CORRELATION_ID = "X-Correlation-Id";

    private Headers() {
    }
}
