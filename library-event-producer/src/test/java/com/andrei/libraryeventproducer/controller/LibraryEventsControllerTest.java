package com.andrei.libraryeventproducer.controller;

import com.andrei.libraryeventproducer.domain.Book;
import com.andrei.libraryeventproducer.domain.LibraryEvent;
import com.andrei.libraryeventproducer.producer.LibraryEventProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
class LibraryEventsControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    LibraryEventProducer libraryEventProducer;

    ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void postLibraryEvent() throws Exception {
        final String json = mapper.writeValueAsString(createLibraryEvent());

        doNothing().when(libraryEventProducer).sendLibraryEvent(ArgumentMatchers.isA(LibraryEvent.class));

        mockMvc.perform(MockMvcRequestBuilders.post("/v1/library-event")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());

    }

    @Test
    void postLibraryEventSynchronous() throws Exception {
        final String json = mapper.writeValueAsString(createLibraryEvent());

        when(libraryEventProducer.sendLibraryEventSynchronousApproach(ArgumentMatchers.isA(LibraryEvent.class))).thenReturn(null);

        mockMvc.perform(MockMvcRequestBuilders.post("/v1/library-event-synchronous")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());

    }

    @Test
    void postLibraryEventWithTopic() throws Exception {
        final String json = mapper.writeValueAsString(createLibraryEvent());

//        when(libraryEventProducer)

        doNothing().when(libraryEventProducer).sendLibraryEventWithTopic(ArgumentMatchers.isA(LibraryEvent.class));

        mockMvc.perform(MockMvcRequestBuilders.post("/v1/library-event-with-topic")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());

    }

    @Test
    void postLibraryEventWithTopicAndHeader() throws Exception {

        final String json = mapper.writeValueAsString(createLibraryEvent());

        doNothing().when(libraryEventProducer).sendLibraryEventWithTopicAndHeader(ArgumentMatchers.isA(LibraryEvent.class));

        mockMvc.perform(MockMvcRequestBuilders.post("/v1/library-event-with-topic-and-header")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());


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
