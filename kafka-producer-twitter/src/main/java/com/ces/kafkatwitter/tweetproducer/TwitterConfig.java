package com.ces.kafkatwitter.tweetproducer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.concurrent.BlockingQueue;

@NoArgsConstructor
public class TwitterConfig {
	
	private static final String API_KEY = "G5UVt1pzTvaUOGDBHjcsAbSqH";

	private static final String API_SECRET_KEY = "ucRwPlJ9cMnVH44GnL9bn4UcrYb2SAuoL4GIDffatEFrm8QhAb";
	
	private static final String ACCESS_TOKEN = "2812959356-pPGfLxdV9rYTcCOJyTBVYqVpN4wHQQDApOZevAd";
	
	private static final String ACCESS_TOKEN_SECRET = "bqNSARUTWfoYxtRbUIkERPLEBjV8Uar0qMsnwoHQbtc4Q";

	private static final List<String> terms = Lists.newArrayList("china", "bitcoin", "modi", "corona");
	public Client getClient(BlockingQueue<String> msgQueue) {
		

		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(API_KEY, API_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);
		
		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));

				return builder.build();
				
	}
}
