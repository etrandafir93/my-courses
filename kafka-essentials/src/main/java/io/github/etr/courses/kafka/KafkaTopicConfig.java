package io.github.etr.courses.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    private static final String STOCK_PRICE_UPDATE_TOPIC = "stock.price.update";

    @Bean
    public NewTopic stockPriceUpdateTopic() {
        return TopicBuilder.name(STOCK_PRICE_UPDATE_TOPIC)
                .partitions(4)
                .replicas(1) // Defaulting to 1 replica as it's common for local/dev setups.
                             // Production environments usually have 3.
                .build();
    }
}
