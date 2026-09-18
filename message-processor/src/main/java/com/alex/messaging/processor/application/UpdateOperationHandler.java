package com.alex.messaging.processor.application;

import com.alex.messaging.event.UpdateRequested;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class UpdateOperationHandler implements OperationHandler<UpdateRequested> {

    private static final Logger log = LoggerFactory.getLogger(UpdateOperationHandler.class);

    private final MessageRepositoryPort repository;

    public UpdateOperationHandler(MessageRepositoryPort repository) {
        this.repository = repository;
    }

    @Override
    public void handle(UpdateRequested event) {
        repository.upsert(event.messageId(), event.msg());
        log.info("applied update id={}", event.messageId());
    }
}
