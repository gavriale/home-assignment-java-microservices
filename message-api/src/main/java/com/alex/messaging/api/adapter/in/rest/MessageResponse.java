package com.alex.messaging.api.adapter.in.rest;

import com.alex.messaging.api.application.MessageView;

public record MessageResponse(int id, String msg) {

    public static MessageResponse from(MessageView view) {
        return new MessageResponse(view.messageId(), view.msg());
    }
}
