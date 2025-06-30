package io.github.etr.courses.kafka.trend.analysis;

import static io.github.etr.courses.kafka.util.LogColors.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

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
    public void analyzeStockPriceUpdate(String message) {
        log.info(green("Received message stock price update message: " + message));

        // Assuming message format "TICKER:PRICE"
        String[] parts = message.split(":");
        String ticker = parts[0];
        double newPrice = Double.parseDouble(parts[1]);
        Double oldPrice = latestStockPrices.get(ticker);

        if (oldPrice == null) {
            log.info(green("New stock ticker {} detected. Initial price: {}"), ticker, newPrice);
        } else if (newPrice > oldPrice) {
            log.info("Stock {} trend: UP 📈 Old Price: {}, New Price: {}",
                ticker, oldPrice, newPrice);
        } else {
            log.info("Stock {} trend: DOWN 📉 Old Price: {}, New Price: {}",
                ticker, oldPrice, newPrice);
        }

        // Simulate other processing time
        Thread.sleep(1000);
        latestStockPrices.put(ticker, newPrice);
    }

}
