package io.github.etr.courses.kafka;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import io.github.etr.courses.kafka.trend.analysis.TrendAnalyzer;

@SpringBootTest
@EmbeddedKafka
class IntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private TrendAnalyzer trendAnalyzer;

    @Value("${topic.stock-price-update}")
    private String stockPriceUpdateTopic;

    @Test
    void shouldSetInitialStockPrice() {
        kafkaTemplate.send(stockPriceUpdateTopic, "AAPL", "AAPL:150.75");

        await().atMost(5, SECONDS)
            .untilAsserted(() ->
                assertThat(trendAnalyzer.getLatestStockPrices())
                    .containsEntry("AAPL", 150.75));
    }

    @Test
    void shouldUpdateLatestStockPriceOnMessage() {
        kafkaTemplate.send(stockPriceUpdateTopic, "GOOGL", "GOOGL:2500.5");
        kafkaTemplate.send(stockPriceUpdateTopic, "GOOGL", "GOOGL:2560.1");
        kafkaTemplate.send(stockPriceUpdateTopic, "GOOGL", "GOOGL:2550.0");

        await().atMost(5, SECONDS)
            .untilAsserted(() ->
                assertThat(trendAnalyzer.getLatestStockPrices())
                    .containsEntry("GOOGL", 2550.0));
    }
}
