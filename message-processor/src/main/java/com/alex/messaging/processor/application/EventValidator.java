package com.alex.messaging.processor.application;

import com.alex.messaging.event.CreateRequested;
import com.alex.messaging.event.DeleteRequested;
import com.alex.messaging.event.UpdateRequested;
import com.alex.messaging.event.WriteRequested;
import com.alex.messaging.processor.domain.exception.InvalidEventException;
import org.springframework.stereotype.Component;

@Component
public class EventValidator {

    public void validate(WriteRequested event) {
        if (event.messageId() <= 0) {
            throw new InvalidEventException("messageId must be positive: " + event.messageId());
        }
        switch (event) {
            case CreateRequested e -> requireNonBlankMsg(e.msg());
            case UpdateRequested e -> requireNonBlankMsg(e.msg());
            case DeleteRequested ignored -> { }
        }
    }

    public void validate(int messageId) {
        if (messageId <= 0) {
            throw new InvalidEventException("messageId must be positive: " + messageId);
        }
    }

    private void requireNonBlankMsg(String msg) {
        if (msg == null || msg.isBlank()) {
            throw new InvalidEventException("msg must not be blank");
        }
    }
}
