package com.alex.messaging.api.adapter.out.kafka;

import com.alex.messaging.api.config.ReplyProperties;
import com.alex.messaging.event.ReadReply;
import com.alex.messaging.topic.Topics;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

/**
 * The reply container manually assigns this instance's single dedicated partition (via
 * {@link TopicPartitionOffset}) rather than joining a consumer group — group rebalancing could
 * otherwise hand the partition to a different instance mid-flight. No group id is set: a reply
 * that outlives an app restart belongs to a request that already timed out on the caller side,
 * so there is nothing worth resuming from a committed offset.
 */
@Configuration
@EnableConfigurationProperties(ReplyProperties.class)
public class KafkaReplyConfig {

    @Bean
    public ConsumerFactory<String, ReadReply> replyConsumerFactory(KafkaProperties kafkaProperties) {
        var props = kafkaProperties.buildConsumerProperties();
        var deserializer = new JacksonJsonDeserializer<>(ReadReply.class);
        deserializer.addTrustedPackages("com.alex.messaging.event");
        deserializer.setUseTypeHeaders(false);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    public KafkaMessageListenerContainer<String, ReadReply> repliesContainer(
            ConsumerFactory<String, ReadReply> replyConsumerFactory, ReplyPartitionResolver partitionResolver) {
        var topicPartition = new TopicPartitionOffset(Topics.READ_REPLY, partitionResolver.partition());
        var containerProperties = new ContainerProperties(topicPartition);
        return new KafkaMessageListenerContainer<>(replyConsumerFactory, containerProperties);
    }

    @Bean
    public ReplyingKafkaTemplate<String, Object, ReadReply> replyingKafkaTemplate(
            ProducerFactory<String, Object> producerFactory,
            KafkaMessageListenerContainer<String, ReadReply> repliesContainer,
            ReplyProperties replyProperties) {
        var template = new ReplyingKafkaTemplate<>(producerFactory, repliesContainer);
        template.setDefaultReplyTimeout(replyProperties.timeout());
        return template;
    }
}
