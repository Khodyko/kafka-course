package io.club.javaholic.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.Headers;
import okhttp3.internal.http2.Header;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;


import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {

        // create Producer props

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");



        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //set safe producer configs (KAFKA<= 2.8)
        properties.setProperty(ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ACKS_CONFIG, "all");
        properties.setProperty(RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        //set high throughput producer config
        properties.setProperty(LINGER_MS_CONFIG, "20");
        properties.setProperty(BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.setProperty(COMPRESSION_TYPE_CONFIG, "snappy");


        // create Producer

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";
        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "http://stream.wikimedia.org/v2/stream/recentchange";

        // Create headers map
        Headers headers = Headers.of("User-Agent", "MyApp/1.0 (contact@example.com)");

        // Build EventSource with headers
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url))
                .headers(headers);
        builder.build().start();

        // we produce for 10 min  and   block the program until then
        TimeUnit.MINUTES.sleep(10);
    }
}
