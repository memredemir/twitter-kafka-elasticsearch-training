import com.twittersample.elasticsearch.consumer.ElasticSearchConsumer;
import com.twittersample.elasticsearch.consumer.ElasticSearchConsumerTransaction;
import com.twittersample.elasticsearch.kafkaconsumer.ElasticSearchKafkaConsumer;
import com.twittersample.elasticsearch.util.TwitterHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

public class Application {


    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(Application.class.getName());
        RestHighLevelClient client = ElasticSearchConsumer.createClient();
        KafkaConsumer<String, String> consumer = ElasticSearchKafkaConsumer.createKafkaConsumer("twitter_tweets");

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            Integer recordCounts = records.count();
            logger.info(String.format("Received %d records!", recordCounts));
            BulkRequest bulkRequest = new BulkRequest();
            for(ConsumerRecord<String, String> record: records) {
                // generate id to make this an idempotent consumer
                // twitter based id is present so don't need to create kafka generic id
                try {
                    String id = TwitterHelper.extractIdFromTweet(record.value());

                    String jsonString = record.value();
                    ElasticSearchConsumerTransaction.addIndexRequestToBulkRequest(bulkRequest, "twitter", jsonString, id);
                }catch (NullPointerException e) {
                    logger.warn(String.format("Skipping bad data: %s", record.value()));
                }
            }
            if(recordCounts > 0){
                BulkResponse bulkResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed.");
            }
        }

        /*
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
         */
    }
}
