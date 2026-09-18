package com.alex.messaging.api.config;

import com.alex.messaging.topic.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * MS-1 provisions the topics it produces to (Create/Update/Delete/Read requests) — the
 * convention followed throughout is that a topic's producer owns its provisioning. MS-2 owns
 * provisioning of the reply and dead-letter topics it produces to.
 */
@Configuration
@EnableConfigurationProperties(TopicProvisioningProperties.class)
public class KafkaTopicConfig {

    @Bean
    NewTopic createTopic(TopicProvisioningProperties properties) {
        return topic(Topics.CREATE, properties);
    }

    @Bean
    NewTopic updateTopic(TopicProvisioningProperties properties) {
        return topic(Topics.UPDATE, properties);
    }

    @Bean
    NewTopic deleteTopic(TopicProvisioningProperties properties) {
        return topic(Topics.DELETE, properties);
    }

    @Bean
    NewTopic readTopic(TopicProvisioningProperties properties) {
        return topic(Topics.READ, properties);
    }

    private NewTopic topic(String name, TopicProvisioningProperties properties) {
        return TopicBuilder.name(name)
                .partitions(properties.partitionCount())
                .replicas(properties.replicationFactor())
                .build();
    }
}
