package com.alex.messaging.api.application;

import com.alex.messaging.event.CreateRequested;
import com.alex.messaging.event.DeleteRequested;
import com.alex.messaging.event.ReadReply;
import com.alex.messaging.event.ReadRequested;
import com.alex.messaging.event.UpdateRequested;

/**
 * Outbound port for publishing events to Kafka. The Kafka implementation lives in
 * {@code adapter/out/kafka} — this interface exists so {@link MessageService} is
 * unit-testable with a fake, no broker required (Ports & Adapters, see CLAUDE.md §5.8).
 */
public interface MessagePublisher {

    void publishCreate(CreateRequested event);

    void publishUpdate(UpdateRequested event);

    void publishDelete(DeleteRequested event);

    ReadReply publishReadAndAwaitReply(ReadRequested event);
}
