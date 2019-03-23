package basic.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerAssignSeek {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ConsumerAssignSeek.class);
        final String bootstrapServer = "127.0.0.1:9092";
        final String groupID = "my-seven-application";
        final String topic = "first_topic";

        // Create consumer config.
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //latest


        // Create consumer
        KafkaConsumer<String, String> consumer  = new KafkaConsumer<String, String>(properties);

        TopicPartition topicPartitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(topicPartitionToReadFrom));

        consumer.seek(topicPartitionToReadFrom, offsetToReadFrom);

        // Subscribe consumer to topic.
        // consumer.subscribe(Collections.singleton(topic)); // only one topic
//        consumer.subscribe(Arrays.asList(topic)); // can add multiple topics.

        int numberOfMessageToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessageReadSofar = 0;

        while (keepOnReading) {
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100)); // New in Kafka 2.0.0

            for (ConsumerRecord<String, String> record : records) {
                logger.info("record key : " + record.key() + " " + " record value : " + record.value());
                logger.info("partition : "+ record.partition() + " record offset : "+ record.offset());
                ++numberOfMessageReadSofar;
                if(numberOfMessageReadSofar >= numberOfMessageToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }

        logger.info("Exit from Application");

    }
}
