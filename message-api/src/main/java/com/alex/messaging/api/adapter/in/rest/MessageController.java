package com.alex.messaging.api.adapter.in.rest;

import com.alex.messaging.api.application.MessageService;
import com.alex.messaging.header.Headers;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST edge for the four operations. Each method only publishes a request to Kafka — none of
 * them touch a database — so a 202 response means "accepted for processing", not "applied".
 */
@RestController
@RequestMapping("/api/v1/messages")
@Validated
@Tag(name = "Messages", description = "Create/Update/Delete/Read requests, published to Kafka for MS-2 to apply")
public class MessageController {

    private final MessageService messageService;

    public MessageController(MessageService messageService) {
        this.messageService = messageService;
    }

    @PostMapping
    @Operation(summary = "Request creation of a message")
    public ResponseEntity<MessageAcceptedResponse> create(@Valid @RequestBody CreateMessageRequest request) {
        var accepted = messageService.create(request.id(), request.msg(), correlationId());
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(MessageAcceptedResponse.from(accepted));
    }

    @PutMapping("/{id}")
    @Operation(summary = "Request an update to an existing message")
    public ResponseEntity<MessageAcceptedResponse> update(@PathVariable @Positive int id,
                                                           @Valid @RequestBody UpdateMessageRequest request) {
        var accepted = messageService.update(id, request.msg(), correlationId());
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(MessageAcceptedResponse.from(accepted));
    }

    @DeleteMapping("/{id}")
    @Operation(summary = "Request deletion of a message")
    public ResponseEntity<MessageAcceptedResponse> delete(@PathVariable @Positive int id) {
        var accepted = messageService.delete(id, correlationId());
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(MessageAcceptedResponse.from(accepted));
    }

    @GetMapping("/{id}")
    @Operation(summary = "Read a message (synchronous request-reply over Kafka, see ADR-003)")
    public ResponseEntity<MessageResponse> read(@PathVariable @Positive int id) {
        var view = messageService.read(id, correlationId());
        return ResponseEntity.ok(MessageResponse.from(view));
    }

    private String correlationId() {
        return MDC.get(Headers.CORRELATION_ID);
    }
}
