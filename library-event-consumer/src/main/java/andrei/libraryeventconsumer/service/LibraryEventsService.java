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
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class LibraryEventsService {

    private final ObjectMapper objectMapper;
    private final LibraryEventsRepository repository;

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
}
























