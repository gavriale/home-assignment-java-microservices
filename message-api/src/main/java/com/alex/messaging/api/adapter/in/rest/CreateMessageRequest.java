package com.alex.messaging.api.adapter.in.rest;

import com.alex.messaging.validation.MessageLimits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

public record CreateMessageRequest(@Positive int id,
                                    @NotBlank @Size(max = MessageLimits.MAX_MSG_LENGTH) String msg) {
}
