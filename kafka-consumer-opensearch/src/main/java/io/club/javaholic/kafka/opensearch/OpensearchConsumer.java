package io.club.javaholic.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpensearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
//        String connString = "http://localhost:9200";
        String connString = "https://823ab74c61:63b25604b6b9ff4648d9@kafka-course-1kmrq7f3.us-east-1.bonsaisearch.net";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static String extractId(String json) {
        // gson library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }


    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpensearchConsumer.class.getSimpleName());
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();
        // create an opensearch client
        RestHighLevelClient opensearchClient = createOpenSearchClient();
        //create index if it doesn't exist
        try (opensearchClient; kafkaConsumer) {

            if (!opensearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT)) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                opensearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            } else {
                log.info("wikimedia index already exists");
            }


            //subbscribe consumer
            kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"));
            try {
            while (true) {

                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));

                    int recordCount = records.count();
                    log.info("Received " + recordCount + " record(s)");

                    BulkRequest bulkRequest = new BulkRequest();

                    for (ConsumerRecord<String, String> record : records) {

                        // send the record into OpenSearch

                        // strategy 1
                        // define an ID using Kafka Record coordinates
//                    String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                        try {
                            // strategy 2
                            // we extract the ID from the JSON value
                            String id = extractId(record.value());

                            IndexRequest indexRequest = new IndexRequest("wikimedia")
                                    .source(record.value(), XContentType.JSON)
                                    .id(id);

//                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                            bulkRequest.add(indexRequest);

//                        log.info(response.getId());
                        } catch (Exception e) {

                        }
                    }
                    if (bulkRequest.numberOfActions() > 0) {
                        BulkResponse bulkResponse = opensearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                        log.info("Inserted: " + bulkResponse.getItems().length + " records.");

                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        kafkaConsumer.commitAsync();
                        log.info("commited");
                    }

        }

            } catch (
                    WakeupException e) {
                log.error("Consumer is starting to shutdown");
            } catch (Exception e) {
                log.error("Unexpected exception in consumer", e);
            } finally {
                kafkaConsumer.close();
                opensearchClient.close();
                log.info("consumer gracefully shut down");
            }
        }
        //close resources

    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupId = "consumer-opensearch-demo";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // create consumer
        return new KafkaConsumer<>(properties);
    }
}
