package com.andrei.libraryeventproducer.producer;

import com.andrei.libraryeventproducer.domain.Book;
import com.andrei.libraryeventproducer.domain.LibraryEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.SettableAnyProperty;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LibraryEventProducerTest {

    @InjectMocks
    private LibraryEventProducer libraryEventProducer;

    @Mock
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    private SettableListenableFuture future;
    private LibraryEvent libraryEvent;

    @BeforeEach
    void setUp() {
        future = new SettableListenableFuture();
        libraryEvent = createLibraryEvent();
    }

    @Test
    void sendLibraryEvent_Success() throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        SendResult<Integer, String> sendResult = getSendResult();
        future.set(sendResult);

        lenient().when(kafkaTemplate.sendDefault(anyInt(), anyString())).thenReturn(future);

        libraryEvent.setLibraryEventId(123);
        final ListenableFuture<SendResult<Integer, String>> sendResultListenableFuture = libraryEventProducer.sendLibraryEvent(libraryEvent);

        final SendResult<Integer, String> sendResult1 = sendResultListenableFuture.get();

        assertEquals(1, sendResult1.getRecordMetadata().partition());
    }

    @Test
    void sendLibraryEvent_Failure() {
        future.setException(new RuntimeException("Exception Calling Kafka"));

        when(kafkaTemplate.sendDefault(anyInt(), anyString())).thenReturn(future);

        assertThrows(Exception.class, () -> libraryEventProducer.sendLibraryEvent(createLibraryEvent()).get());
    }

    @Test
    void sendLibraryEventWithTopic_Success() throws JsonProcessingException, ExecutionException, InterruptedException {

        SendResult<Integer, String> sendResult = getSendResult();
        future.set(sendResult);

        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        final ListenableFuture<SendResult<Integer, String>> sendResultListenableFuture = libraryEventProducer.sendLibraryEventWithTopic(createLibraryEvent());

        final SendResult<Integer, String> sendResult1 = sendResultListenableFuture.get();

        assertEquals(1, sendResult1.getRecordMetadata().partition());
    }

    @Test
    void sendLibraryEventWithTopic_Failure() {
        future.setException(new RuntimeException("Exception Calling Kafka"));

        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        assertThrows(Exception.class, () -> libraryEventProducer.sendLibraryEventWithTopic(createLibraryEvent()).get());
    }

    @Test
    void sendLibraryEventWithTopicAndHeader_Success() throws JsonProcessingException, ExecutionException, InterruptedException {

        SendResult<Integer, String> sendResult = getSendResult();
        future.set(sendResult);

        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        final ListenableFuture<SendResult<Integer, String>> sendResultListenableFuture = libraryEventProducer.sendLibraryEventWithTopicAndHeader(createLibraryEvent());

        final SendResult<Integer, String> sendResult1 = sendResultListenableFuture.get();

        assertEquals(1, sendResult1.getRecordMetadata().partition());
    }

    @Test
    void sendLibraryEventWithTopicAndHeader_Failure() throws JsonProcessingException {

        future.setException(new RuntimeException("Exception Calling Kafka"));

        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        assertThrows(Exception.class, () -> libraryEventProducer.sendLibraryEventWithTopicAndHeader(createLibraryEvent()).get());

    }

    @Test
    void sendLibraryEventSynchronousApproach_Success() throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        SendResult<Integer, String> sendResult = getSendResult();
        future.set(sendResult);

        lenient().when(kafkaTemplate.sendDefault(anyInt(), anyString())).thenReturn(future);

        libraryEvent.setLibraryEventId(123);
        final SendResult<Integer, String> sendResult1 = libraryEventProducer.sendLibraryEventSynchronousApproach(libraryEvent);

        assertEquals(1, sendResult1.getRecordMetadata().partition());
    }

    @Test
    void sendLibraryEventSynchronousApproach_Failure() {
        future.setException(new RuntimeException("Exception Calling Kafka"));

        when(kafkaTemplate.sendDefault(anyInt(), anyString())).thenReturn(future);

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

    private SendResult<Integer, String> getSendResult() throws JsonProcessingException {
        final String record = objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer, String> producerRecord = new ProducerRecord("library-events", libraryEvent.getLibraryEventId(), record);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1),
                1, 1, 123, System.currentTimeMillis(), 1, 2);

        return  new SendResult<>(producerRecord, recordMetadata);
    }
}
