package com.alex.messaging.processor.application;

import com.alex.messaging.event.WriteRequested;

/**
 * Strategy for one write operation. New operation = new implementation of this interface plus
 * one new arm in {@link OperationHandlerRegistry#dispatch} — no other code changes.
 */
public interface OperationHandler<T extends WriteRequested> {

    void handle(T event);
}
