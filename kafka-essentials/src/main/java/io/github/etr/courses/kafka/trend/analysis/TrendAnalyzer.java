package io.github.etr.courses.kafka.trend.analysis;

import static io.github.etr.courses.kafka.util.LogColors.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.kafka.StockPriceUpdate;

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
    public void analyzeStockPriceUpdate(StockPriceUpdate stockPriceUpdate) { // Renamed from StockEvent
        log.info(green("Received stock price update message: {}"), stockPriceUpdate);

        String ticker = stockPriceUpdate.getTicker().toString();
        double newPrice = stockPriceUpdate.getPrice();

        Double oldPrice = latestStockPrices.get(ticker);

        if (oldPrice == null) {
            log.info(green("New stock ticker {} detected. Initial price: {}"), ticker, newPrice);
        } else if (newPrice > oldPrice) {
            log.info(green("Stock {} trend: UP 📈 Old Price: {}, New Price: {}"),
                ticker, oldPrice, newPrice);
        } else if (newPrice < oldPrice) {
            log.info("Stock {} trend: DOWN 📉 Old Price: {}, New Price: {}",
                ticker, oldPrice, newPrice);
        } else {
             log.info("Stock {} trend: NO CHANGE 😐 Price: {}",
                ticker, newPrice);
        }

        Thread.sleep(1000);
        latestStockPrices.put(ticker, newPrice);
    }

}
