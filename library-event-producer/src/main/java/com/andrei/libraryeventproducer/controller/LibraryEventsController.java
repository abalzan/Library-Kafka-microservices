package com.andrei.libraryeventproducer.controller;

import com.andrei.libraryeventproducer.domain.LibraryEvent;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/library-event")
public class LibraryEventsController {

    @PostMapping()
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent event) {

        return ResponseEntity.status(HttpStatus.CREATED).body(event);
    }
}
