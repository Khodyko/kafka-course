package io.club.javaholic.kafka.opensearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

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

    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(OpensearchConsumer.class.getSimpleName());
        // create an opensearch client
        RestHighLevelClient opensearchClient = createOpenSearchClient();
        //create index if it doesn't exist
        try (opensearchClient) {

            if (!opensearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT)) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                opensearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            } else {
                log.info("wikimedia index already exists");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        // creat our kafka client

        //main code logic

        //close resources
    }
}
