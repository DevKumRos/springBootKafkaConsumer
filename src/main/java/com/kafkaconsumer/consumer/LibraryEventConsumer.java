package com.kafkaconsumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class LibraryEventConsumer {

	/* Automatic aknowledging message to broker.*/
	@KafkaListener(topics= {"library-events"})
	public void onMessage(ConsumerRecord<Integer, String> consumerRecord) {
		log.info("Consume Message : {}", consumerRecord);
	}
}
