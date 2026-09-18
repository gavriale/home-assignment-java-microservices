package com.alex.messaging.processor.application;

import com.alex.messaging.event.CreateRequested;
import com.alex.messaging.event.DeleteRequested;
import com.alex.messaging.event.UpdateRequested;
import com.alex.messaging.event.WriteRequested;
import org.springframework.stereotype.Component;

/**
 * Resolves the {@link OperationHandler} Strategy for a given write operation. Implemented as
 * an exhaustive {@code switch} over the sealed {@link WriteRequested} hierarchy rather than a
 * map lookup: a map only fails at runtime if an operation is missing its handler, whereas this
 * fails the build at compile time (CLAUDE.md §5.2) — the property that actually matters here.
 * Adding a sixth operation still costs exactly one new record, one new handler class, and one
 * new arm below.
 */
@Component
public class OperationHandlerRegistry {

    private final CreateOperationHandler createHandler;
    private final UpdateOperationHandler updateHandler;
    private final DeleteOperationHandler deleteHandler;

    public OperationHandlerRegistry(CreateOperationHandler createHandler,
                                     UpdateOperationHandler updateHandler,
                                     DeleteOperationHandler deleteHandler) {
        this.createHandler = createHandler;
        this.updateHandler = updateHandler;
        this.deleteHandler = deleteHandler;
    }

    public void dispatch(WriteRequested event) {
        switch (event) {
            case CreateRequested e -> createHandler.handle(e);
            case UpdateRequested e -> updateHandler.handle(e);
            case DeleteRequested e -> deleteHandler.handle(e);
        }
    }
}
