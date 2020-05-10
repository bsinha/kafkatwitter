package com.ces.kafkatwitter.streams;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonParser;

public class StreamsFilterTweets {
	private static final String bootstrapServer = "127.0.0.1:9092";
	private static final Map<String, Object> configs = ImmutableMap.<String, Object>builder()
			.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
			.put(StreamsConfig.APPLICATION_ID_CONFIG, "Streams-Twitter-Filter")
			.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName())
			.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName()).build();

	public static void main(final String... strings) {
		final StreamsBuilder streamsBuilder = new StreamsBuilder();
		final KStream<String, String> inputStream = streamsBuilder.stream("twitter_tweets");
		inputStream.filter((key, value) -> getTweetsFollower(value) > 10000).to("popular_tweets");

		final Properties properties = new Properties();
		properties.putAll(configs);
		final KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
		kafkaStreams.start();
	}

	private static int getTweetsFollower(final String json) {
		return JsonParser.parseString(json).getAsJsonObject().get("user").getAsJsonObject().get("followers_count")
				.getAsInt();
	}
}
