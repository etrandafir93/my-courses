package io.github.etr.courses.kafka.stock.price;

import static io.github.etr.courses.kafka.util.LogColors.blue;
import static java.util.stream.IntStream.range;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.time.Instant;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.github.etr.courses.kafka.stock.price.StockPriceUpdate; // Updated namespace

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;

@Slf4j
@RestController
@RequestMapping("/api/stocks")
@RequiredArgsConstructor
public class StockPriceProvider {

    @Value("${topic.stock-price-update}")
    private String stockPriceUpdateTopic;

    private final KafkaTemplate<String, StockPriceUpdate> kafkaTemplate;

    @PutMapping("/{ticker}")
    public ResponseEntity<Object> updateStockPrice(@PathVariable String ticker, @RequestParam double price) {
        log.info(blue("Received REST request to update stock price for {}: {}"), ticker, price);
        sendStockUpdate(ticker, price);
        return ResponseEntity.ok().build();
    }

    private void sendStockUpdate(String ticker, double price) {
        log.info(blue("Attempting to send stock update for {}: {}"), ticker, price);

        StockPriceUpdate stockPriceUpdate = StockPriceUpdate.newBuilder()
            .setTicker(ticker)
            .setPrice(price)
            .setTimestamp(Instant.now())
            .build();

        kafkaTemplate.send(stockPriceUpdateTopic, ticker, stockPriceUpdate);
        log.info(blue("Message sent to Kafka topic '{}': Key='{}', Payload='{}'"), stockPriceUpdateTopic, ticker, stockPriceUpdate);
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

    private static double randomPrice() {
        double randomPrice = ThreadLocalRandom.current()
            .nextDouble(140.00, 160.00);
        return Math.round(randomPrice * 100.0) / 100.0;
    }
}
