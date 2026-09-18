package com.alex.messaging.event;

/**
 * The three operations that mutate state and produce no reply. Kept separate from
 * {@link ReadRequested} so MS-2 can exhaustively {@code switch} over just the write
 * operations when dispatching to a {@code Strategy} handler — Read is structurally different
 * (request-reply, no mutation) and is handled by its own listener, not this dispatch.
 */
public sealed interface WriteRequested extends MessageEvent
        permits CreateRequested, UpdateRequested, DeleteRequested {
}
