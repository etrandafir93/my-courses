package io.github.etr.courses.kafka.stock.price;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/stocks")
@RequiredArgsConstructor
public class StockPriceProvider {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String TOPIC_NAME = "stock.price.update";

    @PutMapping("/{ticker}")
    public ResponseEntity<Object> updateStockPrice(@PathVariable String ticker, @RequestParam String price) {
        log.info("Received request to update stock price for {}: {}", ticker, price);
        String messagePayload = ticker + ":" + price;

        try {
            kafkaTemplate.send(TOPIC_NAME, ticker, messagePayload);
            log.info("Message sent to Kafka topic '{}': Key='{}', Payload='{}'", TOPIC_NAME, ticker, messagePayload);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            log.error("Error sending message to Kafka topic '{}' for ticker {}: {}", TOPIC_NAME, ticker, e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

}
