package com.alex.messaging.processor.application;

import com.alex.messaging.event.CreateRequested;
import com.alex.messaging.processor.domain.model.Message;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class CreateOperationHandlerTest {

    private final FakeMessageRepository repository = new FakeMessageRepository();
    private final CreateOperationHandler handler = new CreateOperationHandler(repository);

    @Test
    void insertsANewRow() {
        handler.handle(event(1, "hello"));

        assertThat(repository.findById(1)).contains(new Message(1, "hello"));
    }

    @Test
    void isIdempotentUnderDuplicateDelivery() {
        handler.handle(event(1, "hello"));
        handler.handle(event(1, "hello"));

        assertThat(repository.findById(1)).contains(new Message(1, "hello"));
    }

    private static CreateRequested event(int id, String msg) {
        return new CreateRequested(UUID.randomUUID(), id, msg, Instant.now(), "test-correlation-id");
    }
}
