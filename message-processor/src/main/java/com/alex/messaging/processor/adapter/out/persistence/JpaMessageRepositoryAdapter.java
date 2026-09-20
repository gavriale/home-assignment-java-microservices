package com.alex.messaging.processor.adapter.out.persistence;

import com.alex.messaging.processor.application.MessageRepositoryPort;
import com.alex.messaging.processor.domain.model.Message;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Component
public class JpaMessageRepositoryAdapter implements MessageRepositoryPort {

    private final MessageJpaRepository jpaRepository;
    private final MessageMapper mapper;

    public JpaMessageRepositoryAdapter(MessageJpaRepository jpaRepository, MessageMapper mapper) {
        this.jpaRepository = jpaRepository;
        this.mapper = mapper;
    }

    @Override
    @Transactional
    public void upsert(int id, String msg) {
        jpaRepository.save(new MessageEntity(id, msg));
    }

    @Override
    @Transactional
    public void deleteIfExists(int id) {
        // JpaRepository#deleteById throws if absent; existsById first keeps this a true no-op
        // on a duplicate or out-of-order delete.
        if (jpaRepository.existsById(id)) {
            jpaRepository.deleteById(id);
        }
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<Message> findById(int id) {
        return jpaRepository.findById(id).map(mapper::toDomain);
    }
}
