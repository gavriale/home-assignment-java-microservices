package com.alex.messaging.processor.adapter.out.persistence;

import com.alex.messaging.processor.domain.model.Message;
import org.springframework.stereotype.Component;

/** One implementation, no test double needed — a concrete class, not a port (CLAUDE.md §5.11). */
@Component
public class MessageMapper {

    public Message toDomain(MessageEntity entity) {
        return new Message(entity.getId(), entity.getMsg());
    }
}
