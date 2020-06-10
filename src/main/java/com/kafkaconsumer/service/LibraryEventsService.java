package com.kafkaconsumer.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaconsumer.entity.LibraryEvent;
import com.kafkaconsumer.jpa.LibraryEventsRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LibraryEventsService {
	
	@Autowired
	ObjectMapper objectMapper;
	
	@Autowired
	private LibraryEventsRepository repository;
	
	public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonMappingException, JsonProcessingException {
		LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
		log.info("LibraryEvent is {}", libraryEvent);
		
		switch (libraryEvent.getLibraryEventType()) {
		case NEW:
			saveLibraryEvent(libraryEvent);
			break;
		case UPDATE:
			updateLibraryEvent(libraryEvent);
			break;
		default:
			log.info("Invalid Library Event Type");
		}
	}

	private void saveLibraryEvent(LibraryEvent libraryEvent) {
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		repository.save(libraryEvent);
		log.info("Successfully Persisted the library event : {}",libraryEvent);
	}

	private void updateLibraryEvent(LibraryEvent libraryEvent) {
		if(repository.existsById(libraryEvent.getLibraryEventId()))
		{
			libraryEvent.getBook().setLibraryEvent(libraryEvent);
			repository.save(libraryEvent);
			log.info("Successfully Updated the library event : {}",libraryEvent);
			return;
		}
		log.info("Record not found");
		throw new IllegalArgumentException("No Valid Library Event");
	}
}
