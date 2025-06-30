package io.github.etr.courses.kafka.trend.analysis;

import static io.github.etr.courses.kafka.util.LogColors.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// import org.springframework.beans.factory.annotation.Value; // Value not used anymore
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.kafka.StockEvent; // Import the generated Avro record

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class TrendAnalyzer {

    @Getter
    private final Map<String, Double> latestStockPrices = new ConcurrentHashMap<>();

    @SneakyThrows
    @KafkaListener(topics = "${topic.stock-price-update}", groupId = "trend-analyzers", concurrency = "4")
    // Update to receive StockEvent instead of String
    public void analyzeStockPriceUpdate(StockEvent stockEvent) {
        log.info(green("Received stock price update message: {}"), stockEvent);

        String ticker = stockEvent.getTicker().toString(); // Avro strings can be Utf8
        double newPrice = stockEvent.getPrice();
        // Long timestamp = stockEvent.getTimestamp(); // Timestamp is available if needed

        Double oldPrice = latestStockPrices.get(ticker);

        if (oldPrice == null) {
            log.info(green("New stock ticker {} detected. Initial price: {}"), ticker, newPrice);
        } else if (newPrice > oldPrice) {
            log.info(green("Stock {} trend: UP 📈 Old Price: {}, New Price: {}"),
                ticker, oldPrice, newPrice);
        } else if (newPrice < oldPrice) { // Corrected logic for DOWN trend
            log.info("Stock {} trend: DOWN 📉 Old Price: {}, New Price: {}", // Removed red()
                ticker, oldPrice, newPrice);
        } else {
             log.info("Stock {} trend: NO CHANGE 😐 Price: {}", // Removed yellow()
                ticker, newPrice);
        }

        // Simulate other processing time
        Thread.sleep(1000);
        latestStockPrices.put(ticker, newPrice);
    }

}
