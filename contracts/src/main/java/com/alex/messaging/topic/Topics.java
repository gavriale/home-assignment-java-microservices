package com.alex.messaging.topic;

/**
 * Kafka topic names shared by MS-1 and MS-2. Never inline a topic name in business code — see
 * ADR-001 for why there are four operation topics rather than one.
 */
public final class Topics {

    public static final String CREATE = "messages.create.v1";
    public static final String UPDATE = "messages.update.v1";
    public static final String DELETE = "messages.delete.v1";
    public static final String READ = "messages.read.v1";
    public static final String READ_REPLY = "messages.read.reply.v1";
    public static final String DEAD_LETTER = "messages.dlt.v1";

    private Topics() {
    }
}
