package basic.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {
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

        // Create Producer Record.
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic",
                "Hello World");

        //Send data asynchronous.
        producer.send(record);

        //Add flush to makes a program wait until send complete.
        producer.flush();

        producer.close();

    }
}
