package com.ces.kafkatwitter.avro;

import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import avro.shaded.com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaAvroProducerV2 {

	private static final String bootstrapServer = "127.0.0.1:9092";
	private static final Map<String, Object> configs = ImmutableMap.<String, Object>builder()
			.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer).put(ProducerConfig.ACKS_CONFIG, "1")
			.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))

			.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
			.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
			.put("schema.registry.url", "http://127.0.0.1:8081")

			.build();

	public static void main(final String... strings) {

		final KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<String, Customer>(configs);
		final String topic = "customer-avro_new";
		final Customer customer = Customer.newBuilder().setAge(34).setFirstName("John").setLastName("Doe")
				.setHeight(178f).setWeight(75f).setPhoneNumber("9716476138").setEmail("email@email.com").build();
		final ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(topic, customer);

		kafkaProducer.send(producerRecord, (metadata, exception) -> {
			if (exception != null) {
				log.error(exception.getMessage(), exception);
			} else {
				log.info(metadata.toString());
			}

		});

		kafkaProducer.flush();
		kafkaProducer.close();
	}

}
