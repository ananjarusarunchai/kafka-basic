package basic.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKey {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topic = "first_topic";
        final Logger logger = LoggerFactory.getLogger(ProducerWithKey.class);

        //create Producer properties.
        Properties properties = new Properties();
        String bootstrapServer = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // Create Producer.
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Without any key provided ten Random Partition
        for(int i = 0; i < 10; ++i) {

            String index = Integer.toString(i);

            String value = "Hello World" + index;

            String key = "id_" + index;

            //id_1 partition 1
            //id_2 partition 0
            //id_3 partition 2
            //id_4 partition 0
            //id_5 partition 2
            //id_6 partition 2
            //id_7 partition 0
            //id_8 partition 2
            //id_9 partition 1


            // Create Producer Record.
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("key : " + key);

            //Send data asynchronous.
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //execute every times records successfully send.
                    if (e == null) {
                        logger.info("Receive a new metadata\n" +
                                "\nTopic : " + recordMetadata.topic() +
                                "\nPartition : " + recordMetadata.partition() +
                                "\nOffset : " + recordMetadata.offset() +
                                "\nTimeStamp : " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); // block the .send() to make it synchronous, Don't do this in production.
        }
        //Add flush to makes a program wait until send complete.
        producer.flush();

        producer.close();
    }
}
