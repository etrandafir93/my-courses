package io.github.etr.courses.kafka.stock.price;

import io.github.etr.courses.kafka.util.LogColors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@RestController
@RequestMapping("/api/stocks")
@RequiredArgsConstructor
public class StockPriceProvider {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String TOPIC_NAME = "stock.price.update";

    private void sendStockUpdate(String ticker, String price) {
        log.info(LogColors.colorizeBlue("Attempting to send stock update for {}: {}"), ticker, price);
        String messagePayload = ticker + ":" + price;
        try {
            kafkaTemplate.send(TOPIC_NAME, ticker, messagePayload);
            log.info(LogColors.colorizeBlue("Message sent to Kafka topic '{}': Key='{}', Payload='{}'"), TOPIC_NAME, ticker, messagePayload);
        } catch (Exception e) {
            log.error(LogColors.colorizeBlue("Error sending message to Kafka topic '{}' for ticker {}: {}"), TOPIC_NAME, ticker, e.getMessage(), e);
            // Re-throw or handle as appropriate for internal calls if needed
            throw new RuntimeException("Failed to send Kafka message for " + ticker, e);
        }
    }

    @PutMapping("/{ticker}")
    public ResponseEntity<Object> updateStockPrice(@PathVariable String ticker, @RequestParam String price) {
        log.info(LogColors.colorizeBlue("Received REST request to update stock price for {}: {}"), ticker, price);
        try {
            sendStockUpdate(ticker, price);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            // Logged in sendStockUpdate, additional logging here could be redundant
            // but good for indicating the source of the error in the REST context
            log.error(LogColors.colorizeBlue("Error processing REST request for ticker {}: {}"), ticker, e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void sendInitialStockPrices() {
        log.info(LogColors.colorizeBlue("Application ready. Sending initial stock prices..."));
        List<String> tickers = Arrays.asList(
                "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "BRK.A", "JPM", "JNJ", "V",
                "PG", "UNH", "HD", "MA", "BAC", "DIS", "PYPL", "NFLX", "ADBE", "CRM"
        );

        Random random = ThreadLocalRandom.current();

        for (String ticker : tickers) {
            for (int i = 0; i < 10; i++) {
                // Generate random price between 140 and 160 (e.g., 140.00 to 159.99)
                double randomPrice = 140 + (random.nextDouble() * 20);
                String price = String.format("%.2f", randomPrice);
                try {
                    // Using the refactored method
                    sendStockUpdate(ticker, price);
                    // Optional: add a small delay if needed, but Kafka handles backpressure
                    // Thread.sleep(10);
                } catch (Exception e) {
                    // Logged in sendStockUpdate
                    log.error(LogColors.colorizeBlue("Error sending initial stock price for {} iteration {}: {}"), ticker, i + 1, e.getMessage());
                }
            }
        }
        log.info(LogColors.colorizeBlue("Finished sending initial stock prices. Total messages attempted: {}"), tickers.size() * 10);
    }
}
