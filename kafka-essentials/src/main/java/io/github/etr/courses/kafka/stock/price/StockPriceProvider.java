package io.github.etr.courses.kafka.stock.price;

import static io.github.etr.courses.kafka.util.LogColors.blue;
import static java.util.stream.IntStream.range;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;

@Slf4j
@RestController
@RequestMapping("/api/stocks")
@RequiredArgsConstructor
public class StockPriceProvider {

    @Value("${kafka.topic.stock-price-update}")
    private String stockPriceUpdateTopic;

    private final KafkaTemplate<String, String> kafkaTemplate;

    @PutMapping("/{ticker}")
    public ResponseEntity<Object> updateStockPrice(@PathVariable String ticker, @RequestParam String price) {
        log.info(blue("Received REST request to update stock price for {}: {}"), ticker, price);
        sendStockUpdate(ticker, price);
        return ResponseEntity.ok().build();
    }

    private void sendStockUpdate(String ticker, String price) {
        log.info(blue("Attempting to send stock update for {}: {}"), ticker, price);
        String messagePayload = ticker + ":" + price;

        kafkaTemplate.send(stockPriceUpdateTopic, ticker, messagePayload);
        log.info(blue("Message sent to Kafka topic '{}': Key='{}', Payload='{}'"), stockPriceUpdateTopic, ticker, messagePayload);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void sendInitialStockPrices() {
        log.info(blue("Application ready. Sending initial stock prices..."));
        List<String> tickers = Arrays.asList("AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "BRK.A", "JPM", "JNJ", "BET", "PG", "UNH", "DAX", "VUSA", "BAC",
            "DIS", "PYPL", "NFLX", "ADBE", "CRM");

        range(0, 10).forEach(i ->
            tickers.forEach(ticker ->
                sendStockUpdate(ticker, randomPrice())));

        log.info(blue("Finished sending initial stock prices. Total messages attempted: {}"), tickers.size() * 10);
    }

    private static String randomPrice() {
        double randomPrice = ThreadLocalRandom.current()
            .nextDouble(140.00, 160.00);
        return String.format("%.2f", randomPrice);
    }
}
