package io.github.etr.courses.kafka.stock.price;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/stocks")
@RequiredArgsConstructor
@Slf4j
public class StockPriceProvider {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String TOPIC_NAME = "stock.price.update";

    @PutMapping("/{ticker}/price")
    public void updateStockPrice(@PathVariable String ticker, @RequestParam String price) {
        log.info("Received request to update stock price for {}: {}", ticker, price);
        String messagePayload = ticker + ":" + price;
        try {
            // Sending with ticker as key and "TICKER:PRICE" as value
            kafkaTemplate.send(TOPIC_NAME, ticker, messagePayload);
            log.info("Message sent to Kafka topic '{}': Key='{}', Payload='{}'", TOPIC_NAME, ticker, messagePayload);
        } catch (Exception e) {
            log.error("Error sending message to Kafka topic '{}' for ticker {}: {}", TOPIC_NAME, ticker, e.getMessage(), e);
            // Optionally, rethrow or return an error response
        }
    }
}
