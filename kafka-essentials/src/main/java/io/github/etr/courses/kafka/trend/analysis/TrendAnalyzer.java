package io.github.etr.courses.kafka.trend.analysis;

import static io.github.etr.courses.kafka.util.LogColors.green;
import static java.lang.Double.parseDouble;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class TrendAnalyzer {

    @Getter
    private final Map<String, Double> latestStockPrices = new ConcurrentHashMap<>();

    // TODO
    public void analyzeStockPriceUpdate(String message) {
        // assume message format "ticker:newPrice" eg: "AAPL:150.25"
        log.info(green("Received stock price update message: {}"), message);

        String ticker = message.split(":")[0];
        double newPrice = parseDouble(message.split(":")[1]);

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

//        Thread.sleep(1000); // <-- we can use this to simulate some processing delay
        latestStockPrices.put(ticker, newPrice);
    }

}
