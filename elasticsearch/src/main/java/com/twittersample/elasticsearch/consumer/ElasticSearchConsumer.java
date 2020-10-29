package com.twittersample.elasticsearch.consumer;

import com.twittersample.util.PropertiesOperations;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Properties;

public class ElasticSearchConsumer {

    /**
     *
     * @return RestHighLevelClient.
     */
    public static RestHighLevelClient createClient() throws IOException {
        Properties properties = PropertiesOperations.getProperties();
        String hostname = properties.getProperty("elasticsearch.hostname");
        String username = properties.getProperty("elasticsearch.username");
        String password = properties.getProperty("elasticsearch.password");

        // for remote ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(
                        new RestClientBuilder.HttpClientConfigCallback() {
                            @Override
                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                            }
                        }
                );
        final RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
}
