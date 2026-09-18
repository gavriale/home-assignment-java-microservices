package com.alex.messaging.processor.application;

import com.alex.messaging.event.CreateRequested;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class CreateOperationHandler implements OperationHandler<CreateRequested> {

    private static final Logger log = LoggerFactory.getLogger(CreateOperationHandler.class);

    private final MessageRepositoryPort repository;

    public CreateOperationHandler(MessageRepositoryPort repository) {
        this.repository = repository;
    }

    @Override
    public void handle(CreateRequested event) {
        repository.upsert(event.messageId(), event.msg());
        log.info("applied create id={}", event.messageId());
    }
}
