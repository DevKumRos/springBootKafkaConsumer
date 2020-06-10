package com.kafkaconsumer.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventConsumerManualOffset implements AcknowledgingConsumerAwareMessageListener<Integer, String>{

	/* Manaully aknowledging message to broker. */
	
	@Override
	@KafkaListener(topics= {"library-events"})
	public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment,
			Consumer<?, ?> consumer) {
		log.info(" Manaully aknowledging Consume Message : {}", consumerRecord);
		acknowledgment.acknowledge();
	}
}
