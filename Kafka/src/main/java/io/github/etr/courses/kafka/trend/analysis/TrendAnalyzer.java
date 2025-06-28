package io.github.etr.courses.kafka.trend.analysis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TrendAnalyzer {

    private static final String TOPIC_NAME = "stock.price.update";
    private static final String GROUP_ID = "trend-analyzers"; // Consumer group ID

    @KafkaListener(topics = TOPIC_NAME, groupId = GROUP_ID)
    public void analyzeStockPriceUpdate(String message) {
        log.info("Received message in group '{}' from topic '{}': {}", GROUP_ID, TOPIC_NAME, message);
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
