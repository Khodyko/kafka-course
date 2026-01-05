package io.club.javaholic.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBacks {


    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("hello kafka");

        // create Producer porops

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");


        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");

        // create Producer

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create Proiducer Record

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello_world");

        for(int j=0; j<10; j++) {

            for (int i = 0; i < 30; i++) {
                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //success or error

                        if (e == null) {
                            logger.info("""
                                            Received new metaData
                                            Topic: {}
                                            Partition: {}
                                            Offset: {}
                                            Timestamp: {}
                                            """,
                                    recordMetadata.topic(),
                                    recordMetadata.partition(),
                                    recordMetadata.offset(),
                                    recordMetadata.timestamp()
                            );
                        } else {
                            logger.error(e.getMessage());
                        }
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //flush and close Producer
        producer.flush();
        producer.close();
    }
}
