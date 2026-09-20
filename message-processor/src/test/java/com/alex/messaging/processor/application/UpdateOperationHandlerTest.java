package com.alex.messaging.processor.application;

import com.alex.messaging.event.UpdateRequested;
import com.alex.messaging.processor.domain.model.Message;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class UpdateOperationHandlerTest {

    private final FakeMessageRepository repository = new FakeMessageRepository();
    private final UpdateOperationHandler handler = new UpdateOperationHandler(repository);

    @Test
    void overwritesAnExistingRow() {
        repository.upsert(1, "old");

        handler.handle(event(1, "new"));

        assertThat(repository.findById(1)).contains(new Message(1, "new"));
    }

    @Test
    void alsoUpsertsWhenTheRowDoesNotExistYet() {
        // An Update arriving before its own Create (ADR-004) must not fail - it's the same
        // upsert as Create, so it just creates the row instead of rejecting it.
        handler.handle(event(2, "created by an update"));

        assertThat(repository.findById(2)).contains(new Message(2, "created by an update"));
    }

    private static UpdateRequested event(int id, String msg) {
        return new UpdateRequested(UUID.randomUUID(), id, msg, Instant.now(), "test-correlation-id");
    }
}
