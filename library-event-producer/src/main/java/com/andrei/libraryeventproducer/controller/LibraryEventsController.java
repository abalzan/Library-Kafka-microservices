package com.andrei.libraryeventproducer.controller;

import com.andrei.libraryeventproducer.domain.LibraryEvent;
import com.andrei.libraryeventproducer.producer.LibraryEventProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@RequestMapping("/v1/library-event")
@RequiredArgsConstructor
@Slf4j
public class LibraryEventsController {

    private final LibraryEventProducer libraryEventProducer;

    @PostMapping()
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent event) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

//        libraryEventProducer.sendLibraryEvent(event);
        final SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventSynchronous(event);
        log.info("sendResult is {}", sendResult.toString());
        return ResponseEntity.status(HttpStatus.CREATED).body(event);
    }
}
