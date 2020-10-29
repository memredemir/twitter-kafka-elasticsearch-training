package com.twittersample.twitterkafka.twitter;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twittersample.util.PropertiesOperations;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;



public class TwitterClient {

    /**
     *
     * @param msgQueue Blocking queue of Strings
     * @param terms List of strings for listening on twitter api.
     * @return new hosebirdClient
     */
    public static Client createTwitterClient(BlockingQueue<String> msgQueue, List<String> terms) throws IOException {
        Properties properties = PropertiesOperations.getProperties();
        final String consumerKey = properties.getProperty("twitter.consumer.key");
        final String consumerSecret = properties.getProperty("twitter.consumer.secret");
        final String token = properties.getProperty("twitter.token");
        final String secret = properties.getProperty("twitter.secret");


        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

}
