package com.alex.messaging.processor.application;

import com.alex.messaging.event.CreateRequested;
import com.alex.messaging.event.DeleteRequested;
import com.alex.messaging.event.UpdateRequested;
import com.alex.messaging.processor.domain.exception.InvalidEventException;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * QA pass: EventValidator is the defense-in-depth check on MS-2's side ("never trust the
 * wire", CLAUDE.md 5.9) - it should reject anything message-api's Bean Validation would have
 * rejected, since a Kafka record could in principle come from anywhere, not just message-api.
 */
class EventValidatorTest {

    private final EventValidator validator = new EventValidator();

    @Test
    void rejectsNonPositiveMessageId() {
        assertThatThrownBy(() -> validator.validate(create(0, "hello")))
                .isInstanceOf(InvalidEventException.class);
        assertThatThrownBy(() -> validator.validate(create(-1, "hello")))
                .isInstanceOf(InvalidEventException.class);
    }

    @Test
    void rejectsBlankMsgOnCreateAndUpdate() {
        assertThatThrownBy(() -> validator.validate(create(1, "")))
                .isInstanceOf(InvalidEventException.class);
        assertThatThrownBy(() -> validator.validate(update(1, "   ")))
                .isInstanceOf(InvalidEventException.class);
    }

    @Test
    void deleteNeedsNoMsgCheck() {
        assertThatCode(() -> validator.validate(delete(1))).doesNotThrowAnyException();
    }

    @Test
    void acceptsMsgAtTheDocumentedLimit() {
        String msg = "a".repeat(1000);
        assertThatCode(() -> validator.validate(create(1, msg))).doesNotThrowAnyException();
    }

    @Test
    void rejectsMsgOverTheDocumentedLimit() {
        // message-api's CreateMessageRequest enforces @Size(max = 1000), and MessageEntity's
        // column is length = 1000 - EventValidator currently checks neither, so an oversized
        // msg that somehow reaches this topic (a manual producer, a future second producer,
        // exactly the "never trust the wire" scenario this class exists for) sails through
        // validation and fails later as a raw Postgres column-length violation instead.
        String msg = "a".repeat(1001);
        assertThat(msg).hasSize(1001);

        assertThatThrownBy(() -> validator.validate(create(1, msg)))
                .as("EventValidator should reject an oversized msg the same way message-api's "
                        + "@Size(max = 1000) would, so it's classified as InvalidEventException "
                        + "(non-retryable, straight to DLT) instead of surfacing later as an "
                        + "unclassified DataIntegrityViolationException that gets retried forever")
                .isInstanceOf(InvalidEventException.class);
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
