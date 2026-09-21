package com.alex.messaging.processor.application;

import com.alex.messaging.event.CreateRequested;
import com.alex.messaging.event.DeleteRequested;
import com.alex.messaging.event.UpdateRequested;
import com.alex.messaging.event.WriteRequested;
import com.alex.messaging.processor.domain.exception.InvalidEventException;
import com.alex.messaging.validation.MessageLimits;
import org.springframework.stereotype.Component;

@Component
public class EventValidator {

    public void validate(WriteRequested event) {
        if (event.messageId() <= 0) {
            throw new InvalidEventException("messageId must be positive: " + event.messageId());
        }
        switch (event) {
            case CreateRequested e -> requireValidMsg(e.msg());
            case UpdateRequested e -> requireValidMsg(e.msg());
            case DeleteRequested ignored -> { }
        }
    }

    public void validate(int messageId) {
        if (messageId <= 0) {
            throw new InvalidEventException("messageId must be positive: " + messageId);
        }
    }

    private void requireValidMsg(String msg) {
        if (msg == null || msg.isBlank()) {
            throw new InvalidEventException("msg must not be blank");
        }
        if (msg.length() > MessageLimits.MAX_MSG_LENGTH) {
            throw new InvalidEventException("msg must not exceed " + MessageLimits.MAX_MSG_LENGTH + " characters");
        }
    }
}
