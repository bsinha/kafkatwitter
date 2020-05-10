package com.ces.kafkatwitter.consumer;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonParser;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ElasticKafkaConsumer {

	private static final String HOST_URL = "https://2pagx9bh35:nhhh9g5k2h@kafka-twitter-course-4447475953.ap-southeast-2.bonsaisearch.net:443";// System.getenv("ELASTIC_HOST_URL");
	private static final String KAFKA_BOOTSTRAP_SERVER = "127.0.0.1:9092";
	private static final String KAFKA_GROUP_ID = "kafka-elasticsearch";
	private static final String KAFKA_TOPIC = "twitter_tweets";

	/**
	 * Elastic Search Client to store tweets.
	 * 
	 * @return
	 */
	public static RestHighLevelClient createClient() {

		final URI url = URI.create(HOST_URL);
		final String[] userInfo = url.getUserInfo().split(":");

		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userInfo[0], userInfo[1]));

		final RestHighLevelClient rhlc = new RestHighLevelClient(
				RestClient.builder(new HttpHost(url.getHost(), url.getPort(), url.getScheme()))
						.setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder
								.setDefaultCredentialsProvider(credentialsProvider)
								.setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));

		return rhlc;
	}

	public static KafkaConsumer<String, String> createConsumer(final String topic) {
		final Map<String, Object> configs = ImmutableMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				KAFKA_BOOTSTRAP_SERVER, ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP_ID,
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
				ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(configs);
		kafkaConsumer.subscribe(Arrays.asList(topic));
		return kafkaConsumer;
	}

	public static void main(final String... strings) throws IOException {
		final RestHighLevelClient client = createClient();

		final KafkaConsumer<String, String> kafkaConsumer = createConsumer(KAFKA_TOPIC);
		while (true) {

			kafkaConsumer.poll(Duration.ofMillis(1000)).forEach(record -> {
				try {
					/*
					 * To make consumer idomptent we must use an id in the request. There are 2 ways
					 * to achieve this 1. Use generic id using kafka 2. use id from data. In this
					 * case twitter data has an id field which we can use
					 */
					/* Id from kafka */
					// String id = record.topic() + "_" + record.partition() + "_" +
					// record.offset();

					final String id = extractTweetId(record.value());// record.value() contains full tweet including
																		// tweet id.

					final IndexRequest request = new IndexRequest("twitter").id(id).source(record.value(),
							XContentType.JSON);
					final IndexResponse response = client.index(request, RequestOptions.DEFAULT);
					final String responseId = response.getId();
					log.info(id + " : " + responseId);
					Thread.sleep(1000);
				} catch (final IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					log.error(e.getMessage(), e);
				}

			});
		}

		// client.close();

	}

	private static String extractTweetId(final String json) {
		return JsonParser.parseString(json).getAsJsonObject().get("id_str").getAsString();
	}

}
