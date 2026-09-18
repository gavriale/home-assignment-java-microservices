package com.alex.messaging.processor.config;

import com.alex.messaging.topic.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** MS-2 provisions the topics it produces to: replies now, the dead letter from phase 5. */
@Configuration
@EnableConfigurationProperties(TopicProvisioningProperties.class)
public class KafkaTopicConfig {

    @Bean
    NewTopic readReplyTopic(TopicProvisioningProperties properties) {
        return TopicBuilder.name(Topics.READ_REPLY)
                .partitions(properties.partitionCount())
                .replicas(properties.replicationFactor())
                .build();
    }

    @Bean
    NewTopic deadLetterTopic(TopicProvisioningProperties properties) {
        return TopicBuilder.name(Topics.DEAD_LETTER)
                .partitions(properties.partitionCount())
                .replicas(properties.replicationFactor())
                .build();
    }
}
