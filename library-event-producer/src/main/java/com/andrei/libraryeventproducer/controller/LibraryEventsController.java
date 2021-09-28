package com.andrei.libraryeventproducer.controller;

import com.andrei.libraryeventproducer.domain.LibraryEvent;
import com.andrei.libraryeventproducer.domain.LibraryEventType;
import com.andrei.libraryeventproducer.producer.LibraryEventProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@RequiredArgsConstructor
@Slf4j
public class LibraryEventsController {

    private final LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/library-event")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent event) throws JsonProcessingException {
        event.setLibraryEventType(LibraryEventType.NEW);
        libraryEventProducer.sendLibraryEvent(event);
        return ResponseEntity.status(HttpStatus.CREATED).body(event);
    }

    @PostMapping("/v1/library-event-synchronous")
    public ResponseEntity<LibraryEvent> postLibraryEventSynchronous(@RequestBody LibraryEvent event) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        event.setLibraryEventType(LibraryEventType.NEW);
        final SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventSynchronousApproach(event);
        if(sendResult != null) {
            log.info("sendResult is {}", sendResult.toString());
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(event);
    }

    @PostMapping("/v1/library-event-with-topic")
    public ResponseEntity<LibraryEvent> postLibraryEventWithTopic(@RequestBody LibraryEvent event) throws JsonProcessingException{
        event.setLibraryEventType(LibraryEventType.NEW);
        libraryEventProducer.sendLibraryEventWithTopic(event);
        return ResponseEntity.status(HttpStatus.CREATED).body(event);
    }

    @PostMapping("/v1/library-event-with-topic-and-header")
    public ResponseEntity<LibraryEvent> postLibraryEventWithTopicAndHeader(@RequestBody @Valid LibraryEvent event) throws JsonProcessingException{
        event.setLibraryEventType(LibraryEventType.NEW);
        libraryEventProducer.sendLibraryEventWithTopicAndHeader(event);
        return ResponseEntity.status(HttpStatus.CREATED).body(event);
    }
}
