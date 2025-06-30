package io.github.etr.courses.kafka.trend.analysis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
class TrendAnalyzerTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private TrendAnalyzer trendAnalyzer;

    @Value("${kafka.topic.stock-price-update}")
    private String stockPriceUpdateTopic;

    @Test
    void shouldUpdateLatestStockPriceOnMessage() {
        // Given
        String ticker = "AAPL";
        double price = 150.75;
        String message = ticker + ":" + price;

        // When
        kafkaTemplate.send(stockPriceUpdateTopic, ticker, message);

        // Then
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            Map<String, Double> latestPrices = trendAnalyzer.getLatestStockPrices();
            assertThat(latestPrices).containsKey(ticker);
            assertThat(latestPrices.get(ticker)).isEqualTo(price);
        });
    }

    @Test
    void shouldUpdateTrendCorrectly() {
        // Given
        String ticker = "GOOGL";
        double initialPrice = 2500.50;
        double newPriceUp = 2550.75;
        double newPriceDown = 2400.25;

        String initialMessage = ticker + ":" + initialPrice;
        String upMessage = ticker + ":" + newPriceUp;
        String downMessage = ticker + ":" + newPriceDown;

        // When sending initial price
        kafkaTemplate.send(stockPriceUpdateTopic, ticker, initialMessage);

        // Then assert initial price
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            Map<String, Double> latestPrices = trendAnalyzer.getLatestStockPrices();
            assertThat(latestPrices).containsKey(ticker);
            assertThat(latestPrices.get(ticker)).isEqualTo(initialPrice);
        });

        // When sending a higher price
        kafkaTemplate.send(stockPriceUpdateTopic, ticker, upMessage);

        // Then assert new higher price
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            Map<String, Double> latestPrices = trendAnalyzer.getLatestStockPrices();
            assertThat(latestPrices.get(ticker)).isEqualTo(newPriceUp);
        });

        // When sending a lower price
        kafkaTemplate.send(stockPriceUpdateTopic, ticker, downMessage);

        // Then assert new lower price
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            Map<String, Double> latestPrices = trendAnalyzer.getLatestStockPrices();
            assertThat(latestPrices.get(ticker)).isEqualTo(newPriceDown);
        });
    }
}
