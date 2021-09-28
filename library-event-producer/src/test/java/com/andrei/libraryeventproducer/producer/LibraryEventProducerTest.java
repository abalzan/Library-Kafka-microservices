package com.andrei.libraryeventproducer.producer;

import com.andrei.libraryeventproducer.domain.Book;
import com.andrei.libraryeventproducer.domain.LibraryEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.SettableAnyProperty;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.concurrent.SettableListenableFuture;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class LibraryEventProducerTest {

    @InjectMocks
    private LibraryEventProducer libraryEventProducer;

    @Mock
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
    }

    @Test
    void sendLibraryEvent() {
        SettableListenableFuture future = new SettableListenableFuture();
        future.setException(new RuntimeException("Exception Calling Kafka"));

        Mockito.when(kafkaTemplate.sendDefault(ArgumentMatchers.anyInt(), ArgumentMatchers.anyString())).thenReturn(future);

        assertThrows(Exception.class, () -> libraryEventProducer.sendLibraryEvent(createLibraryEvent()).get());
    }

    @Test
    void sendLibraryEventWithTopic() {
        SettableListenableFuture future = new SettableListenableFuture();
        future.setException(new RuntimeException("Exception Calling Kafka"));

        Mockito.when(kafkaTemplate.send(ArgumentMatchers.isA(ProducerRecord.class))).thenReturn(future);

        assertThrows(Exception.class, () -> libraryEventProducer.sendLibraryEventWithTopic(createLibraryEvent()).get());
    }

    @Test
    void sendLibraryEventWithTopicAndHeader_Failure() throws JsonProcessingException {

        SettableListenableFuture future = new SettableListenableFuture();
        future.setException(new RuntimeException("Exception Calling Kafka"));

        Mockito.when(kafkaTemplate.send(ArgumentMatchers.isA(ProducerRecord.class))).thenReturn(future);

        assertThrows(Exception.class, () -> libraryEventProducer.sendLibraryEventWithTopicAndHeader(createLibraryEvent()).get());

    }

    @Test
    void sendLibraryEventSynchronousApproach() {
        SettableListenableFuture future = new SettableListenableFuture();
        future.setException(new RuntimeException("Exception Calling Kafka"));

        Mockito.when(kafkaTemplate.sendDefault(ArgumentMatchers.anyInt(), ArgumentMatchers.anyString())).thenReturn(future);

        assertThrows(Exception.class, () -> libraryEventProducer.sendLibraryEventSynchronousApproach(createLibraryEvent()));
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
