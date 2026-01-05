package io.club.javaholic.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithCooperative {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("hello kafka consumer");

        String groupId = "my-cons-group";
        String topic = "demo_java";

        // create Producer porps

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");


        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("batch.size", "400");


        properties.setProperty("group.id", groupId);
        // none/earliest/latest
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        // create a consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //get a reference to main thread
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                logger.info("Detexted a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        // subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));



        try {
            //poll for data
            while (true) {
                logger.info("Polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> rec : records) {
                    logger.info("""
                                    key: {},
                                    value:{},
                                    partition: {},
                                    offset: {}
                                    """,
                            rec.key(),
                            rec.value(),
                            rec.partition(),
                            rec.offset());
                }

            }
        } catch (WakeupException e){
            logger.error("Consumer is starting to shutdown");
        } catch (Exception e){
            logger.error("Unexpected exception in consumer", e);
        } finally {
            consumer.close();
            logger.info("consumer gracefully shut down");
        }


    }
}
