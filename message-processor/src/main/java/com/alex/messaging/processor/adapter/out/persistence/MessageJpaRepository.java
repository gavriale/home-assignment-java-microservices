package com.alex.messaging.processor.adapter.out.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

public interface MessageJpaRepository extends JpaRepository<MessageEntity, Integer> {
}
