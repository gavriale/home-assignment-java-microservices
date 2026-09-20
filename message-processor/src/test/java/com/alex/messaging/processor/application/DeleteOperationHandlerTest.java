package com.alex.messaging.processor.application;

import com.alex.messaging.event.DeleteRequested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class DeleteOperationHandlerTest {

    private final FakeMessageRepository repository = new FakeMessageRepository();
    private final DeleteOperationHandler handler = new DeleteOperationHandler(repository);

    @Test
    void removesAnExistingRow() {
        repository.upsert(1, "hello");

        handler.handle(event(1));

        assertThat(repository.findById(1)).isEmpty();
    }

    @Test
    void isANoOpWhenTheRowDoesNotExist() {
        // A duplicate or out-of-order delete must not throw - that's the mechanism behind
        // "Delete is naturally idempotent" (ADR-006), not just an assumption.
        assertThatCode(() -> handler.handle(event(99))).doesNotThrowAnyException();
        assertThat(repository.findById(99)).isEmpty();
    }

    private static DeleteRequested event(int id) {
        return new DeleteRequested(UUID.randomUUID(), id, Instant.now(), "test-correlation-id");
    }
}
