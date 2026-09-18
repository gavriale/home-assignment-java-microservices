package com.alex.messaging.processor.domain.model;

/** MS-2's own view of a message — distinct from both the wire event and the JPA entity. */
public record Message(int id, String msg) {
}
