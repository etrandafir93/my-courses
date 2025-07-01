package io.github.etr.courses.kafka.trend.analysis;

import static io.github.etr.courses.kafka.util.LogColors.green;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import io.github.etr.courses.kafka.stock.price.avro.StockPriceUpdate;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class TrendAnalyzer {

    @Getter
    private final Map<String, Double> latestStockPrices = new ConcurrentHashMap<>();

    @SneakyThrows
    @KafkaListener(topics = "${topic.stock-price-update}", concurrency = "4")
    public void analyzeStockPriceUpdate(StockPriceUpdate message) {
        log.info(green("Received stock price update message: {}"), message);

        String ticker = message.getTicker();
        double newPrice = message.getPrice();

        Double oldPrice = latestStockPrices.get(ticker);

        if (oldPrice == null) {
            log.info(green("New stock ticker {} detected. Initial price: {}"), ticker, newPrice);
        } else if (newPrice > oldPrice) {
            log.info(green("Stock {} trend: UP 📈 Old Price: {}, New Price: {}"),
                ticker, oldPrice, newPrice);
        } else if (newPrice < oldPrice) {
            log.info(green("Stock {} trend: DOWN 📉 Old Price: {}, New Price: {}"),
                ticker, oldPrice, newPrice);
        } else {
             log.info(green("Stock {} trend: NO CHANGE 😐 Price: {}"),
                ticker, newPrice);
        }

//        Thread.sleep(1000);
        latestStockPrices.put(ticker, newPrice);
    }

}
