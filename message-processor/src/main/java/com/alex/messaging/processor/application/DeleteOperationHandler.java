package com.alex.messaging.processor.application;

import com.alex.messaging.event.DeleteRequested;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DeleteOperationHandler implements OperationHandler<DeleteRequested> {

    private static final Logger log = LoggerFactory.getLogger(DeleteOperationHandler.class);

    private final MessageRepositoryPort repository;

    public DeleteOperationHandler(MessageRepositoryPort repository) {
        this.repository = repository;
    }

    @Override
    public void handle(DeleteRequested event) {
        repository.deleteIfExists(event.messageId());
        log.info("applied delete id={}", event.messageId());
    }
}
