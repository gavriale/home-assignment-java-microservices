package com.alex.messaging.processor.config;

import com.alex.messaging.event.CreateRequested;
import com.alex.messaging.event.DeleteRequested;
import com.alex.messaging.event.ReadRequested;
import com.alex.messaging.event.UpdateRequested;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

/**
 * One consumer group and one listener container factory per write operation (CLAUDE.md §5.5):
 * write-heavy and read-heavy paths scale and are monitored independently, at the cost of more
 * consumer groups to operate. {@code AckMode.RECORD} commits the offset only after the
 * listener method returns, i.e. after the DB transaction inside it has already committed —
 * that ordering is what makes "commit after DB success" true without a manual
 * {@code Acknowledgment}.
 */
@Configuration
@EnableConfigurationProperties(ConsumerProvisioningProperties.class)
public class KafkaConsumerConfig {

    private static final String CREATE_GROUP = "processor-create";
    private static final String UPDATE_GROUP = "processor-update";
    private static final String DELETE_GROUP = "processor-delete";
    private static final String READ_GROUP = "processor-read";

    @Bean
    public ConsumerFactory<String, CreateRequested> createConsumerFactory(
            KafkaProperties kafkaProperties, ConsumerProvisioningProperties consumerProperties) {
        return consumerFactory(kafkaProperties, consumerProperties, CREATE_GROUP, CreateRequested.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CreateRequested> createListenerContainerFactory(
            ConsumerFactory<String, CreateRequested> createConsumerFactory,
            ConsumerProvisioningProperties consumerProperties, DefaultErrorHandler errorHandler) {
        return listenerContainerFactory(createConsumerFactory, consumerProperties, errorHandler);
    }

    @Bean
    public ConsumerFactory<String, UpdateRequested> updateConsumerFactory(
            KafkaProperties kafkaProperties, ConsumerProvisioningProperties consumerProperties) {
        return consumerFactory(kafkaProperties, consumerProperties, UPDATE_GROUP, UpdateRequested.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, UpdateRequested> updateListenerContainerFactory(
            ConsumerFactory<String, UpdateRequested> updateConsumerFactory,
            ConsumerProvisioningProperties consumerProperties, DefaultErrorHandler errorHandler) {
        return listenerContainerFactory(updateConsumerFactory, consumerProperties, errorHandler);
    }

    @Bean
    public ConsumerFactory<String, DeleteRequested> deleteConsumerFactory(
            KafkaProperties kafkaProperties, ConsumerProvisioningProperties consumerProperties) {
        return consumerFactory(kafkaProperties, consumerProperties, DELETE_GROUP, DeleteRequested.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, DeleteRequested> deleteListenerContainerFactory(
            ConsumerFactory<String, DeleteRequested> deleteConsumerFactory,
            ConsumerProvisioningProperties consumerProperties, DefaultErrorHandler errorHandler) {
        return listenerContainerFactory(deleteConsumerFactory, consumerProperties, errorHandler);
    }

    @Bean
    public ConsumerFactory<String, ReadRequested> readConsumerFactory(
            KafkaProperties kafkaProperties, ConsumerProvisioningProperties consumerProperties) {
        return consumerFactory(kafkaProperties, consumerProperties, READ_GROUP, ReadRequested.class);
    }

    /**
     * Setting {@code replyTemplate} is what makes the {@code @KafkaListener} method's return
     * value get published automatically to the {@code REPLY_TOPIC}/{@code REPLY_PARTITION}
     * headers carried on the inbound record, correlation id included — no {@code @SendTo}
     * needed since the reply destination is per-request, not fixed (CLAUDE.md §5.3).
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ReadRequested> readListenerContainerFactory(
            ConsumerFactory<String, ReadRequested> readConsumerFactory,
            ConsumerProvisioningProperties consumerProperties,
            DefaultErrorHandler errorHandler,
            KafkaTemplate<String, Object> kafkaTemplate) {
        var factory = listenerContainerFactory(readConsumerFactory, consumerProperties, errorHandler);
        factory.setReplyTemplate(kafkaTemplate);
        return factory;
    }

    /**
     * {@link ErrorHandlingDeserializer} wraps the JSON deserializer so a malformed payload
     * throws inside the listener invocation (where the container's error handler can route it
     * to the DLT) instead of killing the poll loop before the container ever sees it
     * (CLAUDE.md §5.6).
     */
    private <T> ConsumerFactory<String, T> consumerFactory(KafkaProperties kafkaProperties,
                                                            ConsumerProvisioningProperties consumerProperties,
                                                            String groupId, Class<T> targetType) {
        var props = kafkaProperties.buildConsumerProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumerProperties.maxPollRecords());
        var jsonDeserializer = new JacksonJsonDeserializer<>(targetType);
        jsonDeserializer.addTrustedPackages("com.alex.messaging.event");
        jsonDeserializer.setUseTypeHeaders(false);
        var deserializer = new ErrorHandlingDeserializer<>(jsonDeserializer);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> listenerContainerFactory(
            ConsumerFactory<String, T> consumerFactory, ConsumerProvisioningProperties consumerProperties,
            DefaultErrorHandler errorHandler) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, T>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(consumerProperties.concurrency());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
