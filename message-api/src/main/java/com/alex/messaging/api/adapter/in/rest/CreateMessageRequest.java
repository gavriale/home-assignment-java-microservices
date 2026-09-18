package com.alex.messaging.api.adapter.in.rest;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

public record CreateMessageRequest(@Positive int id, @NotBlank @Size(max = 1000) String msg) {
}
