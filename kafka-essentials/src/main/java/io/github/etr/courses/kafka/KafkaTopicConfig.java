package io.github.etr.courses.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
class KafkaTopicConfig {

    @Value("${kafka.topic.stock-price-update}")
    private String stockPriceUpdateTopic;

    @Bean
    public NewTopic stockPriceUpdateTopic() {
        return TopicBuilder.name(stockPriceUpdateTopic)
                .partitions(4)
                .replicas(1)
                .build();
    }
}
