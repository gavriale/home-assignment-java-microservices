package com.alex.messaging.processor.adapter.out.persistence;

import com.alex.messaging.validation.MessageLimits;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** JPA entity — kept distinct from the wire event and the domain model (DTO + Mapper, §5.8). */
@Entity
@Table(name = "messages")
public class MessageEntity {

    @Id
    private Integer id;

    @Column(nullable = false, length = MessageLimits.MAX_MSG_LENGTH)
    private String msg;

    protected MessageEntity() {
    }

    public MessageEntity(Integer id, String msg) {
        this.id = id;
        this.msg = msg;
    }

    public Integer getId() {
        return id;
    }

    public String getMsg() {
        return msg;
    }
}
