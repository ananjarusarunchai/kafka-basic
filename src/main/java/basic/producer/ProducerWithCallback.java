package basic.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

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
            // Create Producer Record.
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic",
                    "Hello World" + i);

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
            });
        }

        //Add flush to makes a program wait until send complete.
        producer.flush();

        producer.close();

    }
}
