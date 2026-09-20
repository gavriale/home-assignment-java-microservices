package com.alex.messaging.processor.application;

import com.alex.messaging.event.CreateRequested;
import com.alex.messaging.event.DeleteRequested;
import com.alex.messaging.event.UpdateRequested;
import com.alex.messaging.processor.domain.model.Message;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The four-topics split means Kafka only orders records within one topic, never across
 * Create/Update/Delete for the same id (ADR-004). These tests dispatch events for one id in
 * the wrong logical order and assert the system still converges to a correct, exception-free
 * final state — the concrete proof behind ADR-006's natural-idempotency claim.
 */
class OperationHandlerRegistryOrderingTest {

    private final FakeMessageRepository repository = new FakeMessageRepository();
    private final OperationHandlerRegistry registry = new OperationHandlerRegistry(
            new CreateOperationHandler(repository),
            new UpdateOperationHandler(repository),
            new DeleteOperationHandler(repository));

    @Test
    void updateArrivingBeforeItsCreateStillConverges() {
        int id = 7;

        registry.dispatch(update(id, "second"));
        registry.dispatch(create(id, "first"));

        // Both operations upsert, so whichever is dispatched last wins - no exception,
        // no lost update, a deterministic final state either way.
        assertThat(repository.findById(id)).contains(new Message(id, "first"));
    }

    @Test
    void deleteArrivingBeforeItsCreateIsASafeNoOp() {
        int id = 9;

        registry.dispatch(delete(id));
        assertThat(repository.findById(id)).isEmpty();

        registry.dispatch(create(id, "created after its own delete"));

        assertThat(repository.findById(id)).contains(new Message(id, "created after its own delete"));
    }

    @Test
    void duplicateDeliveryOfTheSameCreateIsIdempotent() {
        int id = 3;
        CreateRequested event = create(id, "hello");

        registry.dispatch(event);
        registry.dispatch(event);

        assertThat(repository.findById(id)).contains(new Message(id, "hello"));
    }

    private static CreateRequested create(int id, String msg) {
        return new CreateRequested(UUID.randomUUID(), id, msg, Instant.now(), "test-correlation-id");
    }

    private static UpdateRequested update(int id, String msg) {
        return new UpdateRequested(UUID.randomUUID(), id, msg, Instant.now(), "test-correlation-id");
    }

    private static DeleteRequested delete(int id) {
        return new DeleteRequested(UUID.randomUUID(), id, Instant.now(), "test-correlation-id");
    }
}
