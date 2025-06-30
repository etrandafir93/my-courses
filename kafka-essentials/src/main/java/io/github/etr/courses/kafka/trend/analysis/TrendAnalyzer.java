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

    @Value("${kafka.topic.stock-price-update}")
    private String stockPriceUpdateTopic;

    @Getter
    private final Map<String, Double> latestStockPrices = new ConcurrentHashMap<>();

    @SneakyThrows
    @KafkaListener(topics = "${kafka.topic.stock-price-update}", groupId = "trend-analyzers", concurrency = "4")
    public void analyzeStockPriceUpdate(String message) {
        log.info(green("Received message stock price update message: " + message));
        // Assuming message format "TICKER:PRICE"
        String[] parts = message.split(":");
        if (parts.length != 2) {
            log.error(red("Invalid message format: " + message));
            return;
        }
        String ticker = parts[0];
        double newPrice;
        try {
            newPrice = Double.parseDouble(parts[1]);
        } catch (NumberFormatException e) {
            log.error(red("Invalid price format in message: " + message), e);
            return;
        }

        Double oldPrice = latestStockPrices.get(ticker);

        if (oldPrice == null) {
            log.info(yellow("New stock ticker {} detected. Initial price: {}"), ticker, newPrice);
        } else {
            String trendEmoji;
            String trendAnimal;
            if (newPrice > oldPrice) {
                trendEmoji = "📈"; // Arrow up
                trendAnimal = "🐂"; // Bull
                log.info("Stock {} trend: {} UP {} Old Price: {}, New Price: {}", ticker, trendAnimal, trendEmoji, oldPrice, newPrice);
            } else if (newPrice < oldPrice) {
                trendEmoji = "📉"; // Arrow down
                trendAnimal = "🐻"; // Bear
                log.info("Stock {} trend: {} DOWN {} Old Price: {}, New Price: {}", ticker, trendAnimal, trendEmoji, oldPrice, newPrice);
            } else {
                trendEmoji = "📊"; // Chart
                trendAnimal = ""; // No specific animal for stable
                log.info("Stock {} trend: STABLE {} Old Price: {}, New Price: {}", ticker, trendEmoji, oldPrice, newPrice);
            }
        }
        latestStockPrices.put(ticker, newPrice);
        Thread.sleep(1000); // Keep existing sleep for now
    }

}
