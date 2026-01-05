package io.club.javaholic.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {


    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("hello kafka");

        // create Producer porps

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");


        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");

        // create Producer

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create Producer Record

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello_world");

        //send data
        producer.send(producerRecord);
        //flush and close Producer
        producer.flush();
        producer.close();
    }
}
