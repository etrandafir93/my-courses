package io.github.etr.courses.kafka.trend.analysis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Slf4j
@Component
public class TrendAnalyzer {

    private static final String TOPIC_NAME = "stock.price.update";

    @KafkaListener(topics = TOPIC_NAME, groupId = "trend-analyzers")
    public void analyzeStockPriceUpdate(String message) {
        log.info("Received message in group '{}' from topic '{}': {}", "trend-analyzers", TOPIC_NAME, message);
        try {
            // Simulate some processing delay
            Thread.sleep(1000);
            log.info("Finished processing message: {}", message);
        } catch (InterruptedException e) {
            log.error("Error during simulated processing delay for message: {}", message, e);
            Thread.currentThread().interrupt(); // Restore interruption status
        }
    }
}
