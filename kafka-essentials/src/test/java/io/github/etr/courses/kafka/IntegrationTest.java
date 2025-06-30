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
import org.springframework.web.client.RestClient;

import io.github.etr.courses.kafka.trend.analysis.TrendAnalyzer;
import org.junit.jupiter.api.BeforeEach;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka
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
