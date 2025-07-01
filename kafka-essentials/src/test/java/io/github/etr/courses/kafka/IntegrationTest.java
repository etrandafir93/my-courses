package io.github.etr.courses.kafka;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.RestClient;

import io.github.etr.courses.kafka.trend.analysis.TrendAnalyzer;

import org.junit.jupiter.api.BeforeEach;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@EmbeddedKafka
@ActiveProfiles("test")
class IntegrationTest {

    private RestClient restClient;

    @Value("${local.server.port}")
    private int port;

    @Value("${topic.stock-price-update}")
    private String stockPriceUpdateTopic;

    @Autowired
    private TrendAnalyzer trendAnalyzer;

    @BeforeEach
    public void setUp() {
        this.restClient = RestClient.builder()
            .baseUrl("http://localhost:" + port)
            .build();
    }

    @Test
    void shouldSetInitialStockPrice() {
        putPriceUpdate("PLTR", 150.75);

        await().atMost(5, SECONDS)
            .untilAsserted(() ->
                assertThat(trendAnalyzer.getLatestStockPrices())
                    .containsEntry("PLTR", 150.75));
    }

    @Test
    void shouldUpdateLatestStockPriceOnMessage() {
        putPriceUpdate("OKLO", 2553.0);
        putPriceUpdate("OKLO", 2547.5);
        putPriceUpdate("OKLO", 2550.0);

        await().atMost(5, SECONDS)
            .untilAsserted(() ->
                assertThat(trendAnalyzer.getLatestStockPrices())
                    .containsEntry("OKLO", 2550.0));
    }

    private void putPriceUpdate(String ticker, double price) {
        var response = restClient.put()
            .uri("/api/stocks/%s?price=%s".formatted(ticker, price))
            .retrieve()
            .toBodilessEntity();

        assertThat(response.getStatusCode().value())
            .isEqualTo(200);
    }
}
