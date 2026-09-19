package com.alex.messaging.api.adapter.out.kafka;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

/**
 * A producer typed {@code <String, Object>} rather than Boot's auto-configured
 * {@code <?, ?>} template: every record is keyed by the message id as a String (CLAUDE.md
 * §5.1), and the value serializer only needs to handle whichever concrete
 * {@code MessageEvent} the caller passes.
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, Object> producerFactory(KafkaProperties kafkaProperties) {
        var factory = new DefaultKafkaProducerFactory<String, Object>(kafkaProperties.buildProducerProperties());
        factory.setKeySerializer(new StringSerializer());
        factory.setValueSerializer(new JacksonJsonSerializer<>());
        return factory;
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
