package io.github.etr.courses.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
class Config {

    @Bean
    NewTopic stockPriceUpdateTopic(@Value("${topic.stock-price-update}") String topicName) {
        return TopicBuilder.name(topicName)
            .partitions(4)
            .replicas(1)
            .build();
    }
}
