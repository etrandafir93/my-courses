package io.github.etr.courses.kafka.stock.price;

import static io.github.etr.courses.kafka.util.LogColors.blue;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/api/stocks")
@RequiredArgsConstructor
public class StockPriceProvider {

    @Value("${topic.stock-price-update}")
    private String stockPriceUpdateTopic;

    private final KafkaTemplate<String, String> kafkaTemplate;

    @PutMapping("/{ticker}")
    public ResponseEntity<Object> updateStockPrice(@PathVariable String ticker, @RequestParam double price) {
        log.info(blue("Received REST request to update stock price for {}: {}"), ticker, price);
        sendStockUpdate(ticker, price);
        return ResponseEntity.ok().build();
    }

    private void sendStockUpdate(String ticker, double price) {
        log.info(blue("Attempting to send stock update for {}: {}"), ticker, price);

        String stockPriceUpdate = ticker + ":" + price;

        kafkaTemplate.send(stockPriceUpdateTopic, ticker, stockPriceUpdate);
        log.info(blue("Message sent to Kafka topic '{}': Key='{}', Payload='{}'"),
            stockPriceUpdateTopic, ticker, stockPriceUpdate);
    }

}
