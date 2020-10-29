import com.google.common.collect.Lists;
import com.twittersample.twitterkafka.kafka.KafkaConfiguration;
import com.twittersample.twitterkafka.twitter.TwitterClient;
import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Application {

    private Logger logger = LoggerFactory.getLogger(Application.class.getName());

    private Application() {
    }

    public static void main(String[] args) throws IOException {
        new Application().run();
    }


    private void run() throws IOException {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        List<String> terms = Lists.newArrayList("bitcoin");

        Client client = TwitterClient.createTwitterClient(msgQueue, terms);

        // Attempts to establish a connection.
        client.connect();

        // Kafka Producer
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration("localhost:9092", true, true);
        KafkaProducer<String, String> kafkaProducer = kafkaConfiguration.createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shutting down client from twitter...");
            client.stop();
            logger.info("closing producer...");
            kafkaProducer.close();
            logger.info("done!");
        }));

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg != null){
                logger.info(msg);
                kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null) {
                            logger.error("Something bad happened!", e);
                        }

                    }
                });
            }
        }
    }
}
