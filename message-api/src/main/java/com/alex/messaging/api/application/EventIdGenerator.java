package com.alex.messaging.api.application;

import java.util.UUID;

/**
 * Generates event ids. Injected rather than calling {@code UUID.randomUUID()} directly so
 * {@link MessageService} can be unit-tested with a deterministic fake.
 */
public interface EventIdGenerator {

    UUID newEventId();
}
