package com.ces.kafkatwitter.avro;

import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import avro.shaded.com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaAvroConsumerV2 {

	private static final String SCHEMA_REGISTRY_URL = "http://127.0.0.1:8081";
	private static final String KAFKA_BOOTSTRAP_SERVER = "127.0.0.1:9092";
	private static final String KAFKA_GROUP_ID = "my-avro-consumer-2"; // Remember to assign different group id for each
																		// consumer. Otherwise, they will step into each
																		// other.
	private static final String KAFKA_TOPIC = "customer-avro_new";

	public static KafkaConsumer<String, Customer> createConsumer(final String topic) {
		final Map<String, Object> defaultConfigs = ImmutableMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				KAFKA_BOOTSTRAP_SERVER, ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP_ID,
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName(),
				ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		final Map<String, Object> configs = ImmutableMap.<String, Object>builder()
				/* Default config */
				.putAll(defaultConfigs)
				/*
				 * Disable Auto config so that commit is done explicitly and controlled by us.
				 */
				.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false).put("schema.registry.url", SCHEMA_REGISTRY_URL)
				.put("specific.avro.reader", "true").build();

		final KafkaConsumer<String, Customer> kafkaConsumer = new KafkaConsumer<>(configs);
		kafkaConsumer.subscribe(Arrays.asList(topic));
		return kafkaConsumer;
	}

	public static void main(final String... strings) {
		final KafkaConsumer<String, Customer> kafkaConsumer = createConsumer(KAFKA_TOPIC);

		while (true) {

			final ConsumerRecords<String, Customer> records = kafkaConsumer.poll(1000);
			log.info("Received records : " + records.count());
			records.forEach(record -> {
				try {
					final Customer customer = record.value();
					log.info(customer.toString());
					Thread.sleep(10);
				} catch (final InterruptedException e) {
					log.error(e.getMessage(), e);
				}

			});
			log.info("Committing offset...");
			kafkaConsumer.commitSync();

		}

		// kafkaConsumer.close();

	}

}
