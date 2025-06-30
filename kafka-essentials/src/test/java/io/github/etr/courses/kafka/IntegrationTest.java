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

import com.example.kafka.StockEvent; // Import the Avro record
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient; // Added for mock schema registry
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.github.etr.courses.kafka.trend.analysis.TrendAnalyzer;

import org.junit.jupiter.api.BeforeAll; // Changed to BeforeAll for static setup
import org.junit.jupiter.api.BeforeEach;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" } // Standard Kafka properties
)
// Override schema registry URL for tests to use a mock one
@TestPropertySource(properties = {
    "spring.kafka.producer.properties.schema.registry.url=mock://testScope",
    "spring.kafka.consumer.properties.schema.registry.url=mock://testScope",
    "spring.kafka.producer.value-serializer=io.confluent.kafka.serializers.KafkaAvroSerializer",
    "spring.kafka.consumer.value-deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer",
    "spring.kafka.consumer.properties.specific.avro.reader=true"
})
class IntegrationTest {

    // This static client will be picked up by KafkaAvroSerializer/Deserializer
    // when schema.registry.url is "mock://testScope"
    // However, direct registration with the serializer config is more explicit if possible.
    // For now, relying on the mock:// convention.
    // Alternatively, could provide a @TestConfiguration with custom Producer/ConsumerFactory beans.

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
