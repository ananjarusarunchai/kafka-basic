package basic.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThread {
    public static void main(String[] args) {
       new ConsumerWithThread().run();
    }

    public ConsumerWithThread() { }

    private void run() {
        final Logger logger = LoggerFactory.getLogger(ConsumerWithThread.class);
        final String bootstrapServer = "127.0.0.1:9092";
        final String groupID = "my-sixth-application";
        final String topic = "first_topic";
        CountDownLatch countDownLatch = new CountDownLatch(1); // dealing with multi threads.

        Runnable consumerThread = new ConsumerThread(
                bootstrapServer,
                groupID,
                topic,
                countDownLatch);

        Thread thread = new Thread(consumerThread);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) consumerThread).shutdown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("Application is interrupted", e);
        } finally {
            logger.info("Application is closing");
        }

    }

    public class ConsumerThread implements Runnable {

        private CountDownLatch countDownLatch;
        private KafkaConsumer<String, String> consumer;
        final Logger logger = LoggerFactory.getLogger(ConsumerWithThread.class);


        ConsumerThread(String bootstrapServer,
                       String groupID,
                       String topic,
                       CountDownLatch countDownLatch) {
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //latest
            this.consumer  = new KafkaConsumer<String, String>(properties);
            this.countDownLatch = countDownLatch;
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // New in Kafka 2.0.0

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("record key : " + record.key() + " " + " record value : " + record.value());
                        logger.info("partition : " + record.partition() + " record offset : " + record.offset());
                    }
                }
            } catch (WakeupException ex) {
                logger.info("Receive shutdown signal : " + ex);
            } finally {
                consumer.close();
                // tell our main code we're done with consumer.
                countDownLatch.countDown();
            }
        }

        public void shutdown () {
            // wakeup() will interrupt poll()
            // it will throw the exception
            consumer.wakeup();
        }
    }
}
