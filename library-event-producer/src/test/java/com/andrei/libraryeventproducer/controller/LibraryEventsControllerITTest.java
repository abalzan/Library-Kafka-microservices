package com.andrei.libraryeventproducer.controller;

import com.andrei.libraryeventproducer.domain.Book;
import com.andrei.libraryeventproducer.domain.LibraryEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

//property 'spring.embedded.kafka.brokers' come from Spring test library org.springframework.kafka.test.EmbeddedKafkaBroker
//in the case we are just overwriting the properties from application.yml to run the tests.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"
})
class LibraryEventsControllerITTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Integer, String> kafkaConsumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        kafkaConsumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(kafkaConsumer);
    }

    @AfterEach
    void tearDown() {
        kafkaConsumer.close();
    }

    @Test
    @Timeout(5)
    void postLibraryEvent() {
        LibraryEvent libraryEvent = createLibraryEvent();

        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());

        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);

        final ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/library-event", HttpMethod.POST, request, LibraryEvent.class);

        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());
        checkReceivedConsumerNEWMessage();
    }

    @Test
    @Timeout(5)
    void postLibraryEventSynchronous() {
        LibraryEvent libraryEvent = createLibraryEvent();

        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());

        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);

        final ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/library-event-synchronous", HttpMethod.POST, request, LibraryEvent.class);

        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());
        checkReceivedConsumerNEWMessage();
    }

    @Test
    @Timeout(5)
    void postLibraryEventWithTopic() {
        LibraryEvent libraryEvent = createLibraryEvent();

        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());

        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);

        final ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/library-event-with-topic", HttpMethod.POST, request, LibraryEvent.class);

        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());
        checkReceivedConsumerNEWMessage();
    }

    @Test
    @Timeout(5)
    void postLibraryEventWithTopicAndHeader() {

        LibraryEvent libraryEvent = createLibraryEvent();

        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());

        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);

        final ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/library-event-with-topic-and-header", HttpMethod.POST, request, LibraryEvent.class);

        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

        checkReceivedConsumerNEWMessage();

    }


    @Test
    @Timeout(5)
    void putLibraryEventWithTopicAndHeader() {

        LibraryEvent libraryEvent = createLibraryEvent();
        libraryEvent.setLibraryEventId(123);

        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());

        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);

        final ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/library-event-with-topic-and-header", HttpMethod.PUT, request, LibraryEvent.class);

        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());

        checkReceivedConsumerUPDATEMessage();

    }

    private void checkReceivedConsumerUPDATEMessage() {
        final ConsumerRecord<Integer, String> singleRecord = KafkaTestUtils.getSingleRecord(kafkaConsumer, "library-events");
        String expected = "{\"libraryEventId\":123,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka using Spring Boot\",\"bookAuthor\":\"Andrei\"}}";
        final String value = singleRecord.value();
        assertEquals(expected, value);
    }

    private void checkReceivedConsumerNEWMessage() {
        final ConsumerRecord<Integer, String> singleRecord = KafkaTestUtils.getSingleRecord(kafkaConsumer, "library-events");
        String expected = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka using Spring Boot\",\"bookAuthor\":\"Andrei\"}}";
        final String value = singleRecord.value();
        assertEquals(expected, value);
    }

    private LibraryEvent createLibraryEvent() {
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Andrei")
                .bookName("Kafka using Spring Boot")
                .build();

        return LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
    }
}
