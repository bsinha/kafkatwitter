package com.ces.kafkatwitter.tweetproducer;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

public class KafkaConfig {
	private static final String bootstrapServer = "127.0.0.1:9092";
	private static final Map<String, Object> configs = ImmutableMap.<String, Object>builder()
			.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
			.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
			.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
			
			/* Creates safe idempotent producer */
			.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
			/* Not needed to set explicitly after idempotence setting, but a good practice*/
			.put(ProducerConfig.ACKS_CONFIG, "all")
			.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,5)
			.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
			/* Recommended: High Throughput producer ( at the expense of latency and CPU usage) */
			.put(ProducerConfig.LINGER_MS_CONFIG, 20)
			.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy") /* Compression library from Google. You can also use lz4. */
			.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 *1024)) /* Optimum value: 32 KB .*/
			.build();
	
	
	public static KafkaProducer<String, String> getProducer() {
		return new KafkaProducer<String, String>(configs);
	}

}
