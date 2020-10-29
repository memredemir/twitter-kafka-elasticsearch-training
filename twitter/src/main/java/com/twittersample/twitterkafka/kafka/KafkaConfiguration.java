package com.twittersample.twitterkafka.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaConfiguration {

    private String bootstrapServer = "localhost:9092";
    private boolean isSafeProducer = false;
    private boolean isHighThroughputProducer = false;

    public KafkaConfiguration() {
    }

    public KafkaConfiguration(String bootstrapServer, boolean isSafeProducer, boolean isHighThroughputProducer) {
        this.bootstrapServer = bootstrapServer;
        this.isSafeProducer = isSafeProducer;
        this.isHighThroughputProducer = isHighThroughputProducer;
    }

    /**
     *
     * @return a kafka producer.
     */
    public KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        if(this.isSafeProducer){
            // create safe producer
            properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
            properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
            properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        }

        if(this.isHighThroughputProducer) {
            // high throughput producer (at the expanse of a bit of latency and CPU usage)
            properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
            properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
            properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32Kb batch size
        }


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return producer;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public void setBootstrapServer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    public boolean isSafeProducer() {
        return isSafeProducer;
    }

    public void setSafeProducer(boolean safeProducer) {
        isSafeProducer = safeProducer;
    }

    public boolean isHighThroughputProducer() {
        return isHighThroughputProducer;
    }

    public void setHighThroughputProducer(boolean highThroughputProducer) {
        isHighThroughputProducer = highThroughputProducer;
    }
}
