package com.ces.kafkatwitter.tweetproducer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.twitter.hbc.core.Client;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@Slf4j
public class Application {

	public static void main(String... args) {
		new Application().run();

	}

	private void run() {
		log.info("Start application");

		/* Create Twitter client */
		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		TwitterConfig twitter = new TwitterConfig();
		Client twitterClient = twitter.getClient(msgQueue);
		twitterClient.connect();
		
		KafkaProducer<String, String> kafkaProducer = KafkaConfig.getProducer();
		
		Runtime.getRuntime().addShutdownHook(new Thread( () ->  {
			log.info("stopping application");
			log.info("shutting down client from twitter...");
			twitterClient.stop();
			log.info("closing producer");
			kafkaProducer.close();
			log.info("done !");
		}));
		// on a different thread, or multiple different threads....
		while (!twitterClient.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				log.error(e.getMessage(), e);
				twitterClient.stop();

			}
			if (msg != null) {
				log.info(msg);
				//System.out.println(msg);
				kafkaProducer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
					
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if(exception != null) {
							log.error(exception.getMessage(), exception);
						}
						
					}
				});
			}

		}
		//log.info("Application ends...");
	}
}
