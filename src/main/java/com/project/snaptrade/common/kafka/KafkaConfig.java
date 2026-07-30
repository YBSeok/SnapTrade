package com.project.snaptrade.common.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;

@EnableKafka
@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic tradeCompletedTopic() {
        return TopicBuilder.name(KafkaTopics.TRADE_COMPLETED)
                .partitions(3)
                .replicas(2)
                .build();
    }

    @Bean
    public NewTopic orderLifecycleTopic() {
        return TopicBuilder.name(KafkaTopics.ORDER_LIFECYCLE)
                .partitions(3)
                .replicas(2)
                .build();
    }

    @Bean
    public NewTopic orderProjectionTopic() {
        return TopicBuilder.name(KafkaTopics.ORDER_PROJECTION)
                .partitions(3)
                .replicas(2)
                .build();
    }
}
