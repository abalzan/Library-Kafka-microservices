package com.andrei.libraryeventproducer.controller;

import com.andrei.libraryeventproducer.domain.Book;
import com.andrei.libraryeventproducer.domain.LibraryEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

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

    @BeforeEach
    void setUp() {
    }

    @Test
    void postLibraryEvent() {
        LibraryEvent libraryEvent = createLibraryEvent();

        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());

        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);

        final ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/library-event", HttpMethod.POST, request, LibraryEvent.class);

        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());
    }

    @Test
    void postLibraryEventSynchronous() {
        LibraryEvent libraryEvent = createLibraryEvent();

        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());

        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);

        final ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/library-event-synchronous", HttpMethod.POST, request, LibraryEvent.class);

        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());
    }

    @Test
    void postLibraryEventWithTopic() {
        LibraryEvent libraryEvent = createLibraryEvent();

        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());

        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);

        final ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/library-event-with-topic", HttpMethod.POST, request, LibraryEvent.class);

        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());
    }

    @Test
    void postLibraryEventWithTopicAndHeader() {

        LibraryEvent libraryEvent = createLibraryEvent();

        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());

        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);

        final ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/library-event-with-topic-and-header", HttpMethod.POST, request, LibraryEvent.class);

        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

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
