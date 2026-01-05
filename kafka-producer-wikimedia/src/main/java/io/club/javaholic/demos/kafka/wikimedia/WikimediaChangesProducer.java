package io.club.javaholic.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.Headers;
import okhttp3.internal.http2.Header;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;


import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {

        // create Producer props

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");


        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");

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
