package io.github.etr.courses.kafka;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource; // Added for overriding properties
import org.springframework.web.client.RestClient;

// Import for StockPriceUpdate will be automatically handled if Avro plugin generates it in the correct package
// For now, assuming com.example.kafka.StockPriceUpdate will be the FQN
// No direct import of StockPriceUpdate needed in this test file if not directly referenced.
// However, if IntegrationTest were to, for example, send a StockPriceUpdate object directly via a test producer,
// then an import for com.example.kafka.StockPriceUpdate would be needed.

import io.github.etr.courses.kafka.trend.analysis.TrendAnalyzer;

import org.junit.jupiter.api.BeforeEach;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" }
)
// @TestPropertySource properties will be moved to src/test/resources/application.yaml
class IntegrationTest {

    @Autowired
    private RestClient.Builder restClientBuilder;

    private RestClient restClient;

    @Autowired
    private TrendAnalyzer trendAnalyzer;

    @Value("${local.server.port}")
    private int port;

    @Value("${topic.stock-price-update}")
    private String stockPriceUpdateTopic;

    @BeforeEach
    public void setUp() {
        this.restClient = restClientBuilder.baseUrl("http://localhost:" + port).build();
    }

    @Test
    void shouldSetInitialStockPrice() {
        restClient.put()
            .uri("/api/stocks/AAPL?price=150.75")
            .contentType(MediaType.APPLICATION_JSON)
            .retrieve()
            .toBodilessEntity();

        await().atMost(5, SECONDS)
            .untilAsserted(() ->
                assertThat(trendAnalyzer.getLatestStockPrices())
                    .containsEntry("AAPL", 150.75));
    }

    @Test
    void shouldUpdateLatestStockPriceOnMessage() {
        restClient.put()
            .uri("/api/stocks/GOOGL?price=2500.5")
            .contentType(MediaType.APPLICATION_JSON)
            .retrieve()
            .toBodilessEntity();
        restClient.put()
            .uri("/api/stocks/GOOGL?price=2560.1")
            .contentType(MediaType.APPLICATION_JSON)
            .retrieve()
            .toBodilessEntity();
        restClient.put()
            .uri("/api/stocks/GOOGL?price=2550.0")
            .contentType(MediaType.APPLICATION_JSON)
            .retrieve()
            .toBodilessEntity();

        await().atMost(5, SECONDS)
            .untilAsserted(() ->
                assertThat(trendAnalyzer.getLatestStockPrices())
                    .containsEntry("GOOGL", 2550.0));
    }
}
