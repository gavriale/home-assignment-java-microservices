package com.alex.messaging.processor.application;

import com.alex.messaging.processor.domain.model.Message;

import java.util.Optional;

/**
 * Outbound port to the database. The JPA implementation lives in
 * {@code adapter/out/persistence} — this interface exists so the operation handlers are
 * unit-testable with a fake, no database required (Ports & Adapters, CLAUDE.md §5.8).
 */
public interface MessageRepositoryPort {

    /** Create and Update both upsert — naturally idempotent under at-least-once delivery. */
    void upsert(int id, String msg);

    /** Delete-if-exists — a duplicate or out-of-order delete is a harmless no-op. */
    void deleteIfExists(int id);

    Optional<Message> findById(int id);
}
