package andrei.libraryeventconsumer.service;

import andrei.libraryeventconsumer.entity.LibraryEvent;
import andrei.libraryeventconsumer.entity.LibraryEventType;
import andrei.libraryeventconsumer.repository.LibraryEventsRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
@Slf4j
@RequiredArgsConstructor
public class LibraryEventsService {

    private final ObjectMapper objectMapper;
    private final LibraryEventsRepository repository;
    private final KafkaTemplate<Integer, String> kafkaTemplate;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        final LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("libraryEvent {}", libraryEvent);

        //Emulating recoverable scenario
        if(libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId().equals(111)) {
            throw new RecoverableDataAccessException("Temporary network issue");
        }

        save(libraryEvent);
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        if(libraryEvent.getLibraryEventType().equals(LibraryEventType.UPDATE)) {
            validate(libraryEvent);
        }
        repository.save(libraryEvent);
        log.info("Successfully Persisted the library event {} ", libraryEvent);
    }

    private void validate(LibraryEvent libraryEvent) {
        repository.findById(libraryEvent.getLibraryEventId()).ifPresentOrElse(this::save, () -> { throw new IllegalArgumentException("Not a valid Library Event");});
    }

    public void handleRecovery(ConsumerRecord<Integer, String> consumerRecord) {
        Integer key = consumerRecord.key();
        String message = consumerRecord.value();
        final ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, message);
        getCallbackListenableFuture(key, message, listenableFuture);

    }

    private ListenableFuture<SendResult<Integer, String>> getCallbackListenableFuture(Integer key, String value, ListenableFuture<SendResult<Integer, String>> sendResultListenableFuture) {
        sendResultListenableFuture.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable throwable) {
                handleFailure(key, value, throwable);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {

                handleSuccess(key, value, result);
            }
        });
        return sendResultListenableFuture;
    }


    private void handleFailure(Integer key, String value, Throwable throwable) {
        log.error("Error sending the Message and the exception is {}", throwable.getMessage());
        try {
            throw throwable;
        } catch (Throwable ex) {
            log.error("Error on Failure {}", ex.getMessage());
        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message Sent Successfully for the key: {} and the value is {}, partition is: {} ", key, value, result.getRecordMetadata().partition());
    }
}
























