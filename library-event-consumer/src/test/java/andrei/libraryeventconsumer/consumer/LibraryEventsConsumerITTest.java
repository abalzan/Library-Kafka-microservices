package andrei.libraryeventconsumer.consumer;

import andrei.libraryeventconsumer.entity.Book;
import andrei.libraryeventconsumer.entity.LibraryEvent;
import andrei.libraryeventconsumer.entity.LibraryEventType;
import andrei.libraryeventconsumer.repository.LibraryEventsRepository;
import andrei.libraryeventconsumer.service.LibraryEventsService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
class LibraryEventsConsumerITTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry endpointRegistry;

    @Autowired
    private LibraryEventsRepository libraryEventsRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @SpyBean
    private LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    private LibraryEventsService libraryEventsServiceSpy;

    @BeforeEach
    void setUp() {
        endpointRegistry.getAllListenerContainers()
                .forEach(messageListenerContainer -> ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic())
                );
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @SneakyThrows
    @Test
    void publishNewLibraryEvent() {

        CountDownLatch latch = new CountDownLatch(1);

        String json = """
                {
                    "libraryEventId": null,
                    "libraryEventType": "NEW",
                    "book": {
                        "bookId": 123,
                        "bookName": "My kafka book",
                        "bookAuthor": "Andrei"
                    }
                }
                """;
        kafkaTemplate.sendDefault(json).get();
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        final List<LibraryEvent> libraryEventList = libraryEventsRepository.findAll();
        assertEquals(1, libraryEventList.size());

        libraryEventList.forEach(libraryEvent -> {
            assertNotNull(libraryEvent.getLibraryEventId());
            assertEquals(123, libraryEvent.getBook().getBookId());
        });
    }

    @SneakyThrows
    @Test
    void publishUpdateLibraryEvent() {
        CountDownLatch latch = new CountDownLatch(1);

        String json = """
                {
                    "libraryEventId": null,
                    "libraryEventType": "NEW",
                    "book": {
                        "bookId": 123,
                        "bookName": "My kafka book",
                        "bookAuthor": "Andrei"
                    }
                }
                """;
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        //publish Update library Event
        Book updatedBook = Book.builder()
                .bookId(123)
                .bookName("My Updated Book")
                .bookAuthor("Updated Author")
                .build();

        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

        latch.await(3, TimeUnit.SECONDS);

        final LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();

        assertEquals("My Updated Book", persistedLibraryEvent.getBook().getBookName());
        assertEquals("Updated Author", persistedLibraryEvent.getBook().getBookAuthor());

    }

    @SneakyThrows
    @Test
    void publishModifyLibraryEvent_Not_A_Valid_LibraryEventId() {
        //given
        String json = """
                {
                    "libraryEventId": 123,
                    "libraryEventType": "UPDATE",
                    "book": {
                        "bookId": 123,
                        "bookName": "My kafka book",
                        "bookAuthor": "Andrei"
                    }
                }
                """;
        System.out.println(json);
        kafkaTemplate.sendDefault(123, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);


        verify(libraryEventsConsumerSpy, atLeast(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, atLeast(1)).processLibraryEvent(isA(ConsumerRecord.class));

        Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(123);
        assertFalse(libraryEventOptional.isPresent());
    }

    @SneakyThrows
    @Test
    void publishModifyLibraryEvent_Null_LibraryEventId() {
        //given
        String json = """
                {
                    "libraryEventId": null,
                    "libraryEventType": "UPDATE",
                    "book": {
                        "bookId": 123,
                        "bookName": "My kafka book",
                        "bookAuthor": "Andrei"
                    }
                }
                """;
        kafkaTemplate.sendDefault(null, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));
    }

}
