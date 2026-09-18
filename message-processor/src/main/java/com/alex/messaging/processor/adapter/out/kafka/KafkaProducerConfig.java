package com.alex.messaging.processor.adapter.out.kafka;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * MS-2's producer side: replies on {@code messages.read.reply.v1} (via the listener container
 * factory's {@code replyTemplate}) and, from phase 5, dead letters on {@code messages.dlt.v1}.
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, Object> producerFactory(KafkaProperties kafkaProperties) {
        var factory = new DefaultKafkaProducerFactory<String, Object>(kafkaProperties.buildProducerProperties());
        factory.setKeySerializer(new StringSerializer());
        factory.setValueSerializer(new JsonSerializer<>());
        return factory;
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
