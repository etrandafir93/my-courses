package io.github.etr.courses.kafka.trend.analysis;

import static io.github.etr.courses.kafka.util.LogColors.green;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class TrendAnalyzer {

    private static final String STOCK_PRICE_UPDATE_TOPIC = "stock.price.update";

    @SneakyThrows
    @KafkaListener(topics = STOCK_PRICE_UPDATE_TOPIC, groupId = "trend-analyzers")
    public void analyzeStockPriceUpdate(String message) {
        log.info(green("Received message stock price update message: " + message));
        Thread.sleep(1000);
    }

}
