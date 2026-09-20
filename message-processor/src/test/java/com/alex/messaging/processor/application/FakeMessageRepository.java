package com.alex.messaging.processor.application;

import com.alex.messaging.processor.domain.model.Message;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Hand-rolled in-memory fake — the second real implementation of {@link MessageRepositoryPort}
 * that is the actual reason this port is an interface: no database, no Spring context, no
 * mocking framework needed to exercise the handlers.
 */
final class FakeMessageRepository implements MessageRepositoryPort {

    private final Map<Integer, String> rows = new LinkedHashMap<>();

    @Override
    public void upsert(int id, String msg) {
        rows.put(id, msg);
    }

    @Override
    public void deleteIfExists(int id) {
        rows.remove(id);
    }

    @Override
    public Optional<Message> findById(int id) {
        return Optional.ofNullable(rows.get(id)).map(msg -> new Message(id, msg));
    }
}
