package com.alex.messaging.api.adapter.in.rest;

import com.alex.messaging.api.domain.exception.MessageNotFoundException;
import com.alex.messaging.api.domain.exception.PublishFailedException;
import com.alex.messaging.api.domain.exception.ReplyTimeoutException;
import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.stream.Collectors;

/**
 * One place mapping every exception to an RFC 7807 {@link ProblemDetail} — consistent shape
 * for every error, no stack traces in responses.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleBodyValidation(MethodArgumentNotValidException ex) {
        String detail = ex.getBindingResult().getFieldErrors().stream()
                .map(this::describe)
                .collect(Collectors.joining("; "));
        return problem(HttpStatus.BAD_REQUEST, "Validation failed", detail);
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ProblemDetail handleParameterValidation(ConstraintViolationException ex) {
        return problem(HttpStatus.BAD_REQUEST, "Validation failed", ex.getMessage());
    }

    @ExceptionHandler(MessageNotFoundException.class)
    public ProblemDetail handleNotFound(MessageNotFoundException ex) {
        return problem(HttpStatus.NOT_FOUND, "Message not found", ex.getMessage());
    }

    @ExceptionHandler(ReplyTimeoutException.class)
    public ProblemDetail handleReplyTimeout(ReplyTimeoutException ex) {
        log.warn(ex.getMessage(), ex);
        return problem(HttpStatus.GATEWAY_TIMEOUT, "Read timed out", ex.getMessage());
    }

    @ExceptionHandler(PublishFailedException.class)
    public ProblemDetail handlePublishFailed(PublishFailedException ex) {
        log.error("publish to Kafka failed", ex);
        return problem(HttpStatus.SERVICE_UNAVAILABLE, "Publish failed", ex.getMessage());
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpected(Exception ex) {
        log.error("unhandled exception", ex);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "Internal error", "An unexpected error occurred");
    }

    private String describe(FieldError error) {
        return "%s %s".formatted(error.getField(), error.getDefaultMessage());
    }

    private ProblemDetail problem(HttpStatus status, String title, String detail) {
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(status, detail);
        problemDetail.setTitle(title);
        return problemDetail;
    }
}
