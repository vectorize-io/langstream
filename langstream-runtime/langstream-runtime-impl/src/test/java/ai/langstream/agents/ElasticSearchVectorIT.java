/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.agents;

import static ai.langstream.testrunners.AbstractApplicationRunner.INTEGRATION_TESTS_GROUP1;

import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.testrunners.AbstractGenericStreamingApplicationRunner;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
@Tag(INTEGRATION_TESTS_GROUP1)
class ElasticSearchVectorIT extends AbstractGenericStreamingApplicationRunner {
    @Container
    static ElasticsearchContainer ELASTICSEARCH =
            new ElasticsearchContainer(
                            markAsDisposableImage(
                                    DockerImageName.parse(
                                            "docker.elastic.co/elasticsearch/elasticsearch:8.14.0")))
                    .withEnv("discovery.type", "single-node")
                    .withEnv("xpack.security.enabled", "false")
                    .withEnv("xpack.security.http.ssl.enabled", "false")
                    .withLogConsumer(
                            frame -> log.info("elasticsearch > {}", frame.getUtf8String()));

    @Test
    @Disabled
    public void testElasticCloud() throws Exception {
        test(true, "xx.es.us-east-1.aws.elastic.cloud", 443, "==");
    }

    @Test
    public void testElastic8() throws Exception {
        test(false, "localhost", ELASTICSEARCH.getMappedPort(9200), null);
    }

    public void test(boolean https, String host, int port, String apiKey) throws Exception {
        Map<String, String> application =
                Map.of(
                        "configuration.yaml",
                        """
                                configuration:
                                  resources:
                                    - type: "vector-database"
                                      name: "ESDatasource"
                                      configuration:
                                        service: "elasticsearch"
                                        https: %s
                                        host: "%s"
                                        port: "%d"
                                        api-key: %s
                                """
                                .formatted(https, host, port, apiKey),
                        "pipeline-write.yaml",
                        """
                                topics:
                                  - name: "insert-topic"
                                    creation-mode: create-if-not-exists
                                assets:
                                  - name: "es-index"
                                    asset-type: "elasticsearch-index"
                                    creation-mode: create-if-not-exists
                                    config:
                                       datasource: "ESDatasource"
                                       index: my-index-000
                                       settings: |
                                           {
                                                "index": {}
                                            }
                                       mappings: |
                                           {
                                                "properties": {
                                                      "content": {
                                                            "type": "text"
                                                      },
                                                      "embeddings": {
                                                            "type": "dense_vector",
                                                            "dims": 3,
                                                            "similarity": "cosine"
                                                      }
                                                }
                                            }
                                pipeline:
                                  - id: write
                                    name: "Write"
                                    type: "vector-db-sink"
                                    input: "insert-topic"
                                    configuration:
                                      datasource: "ESDatasource"
                                      index: "{{{ properties.index_name }}}"
                                      id: "key"
                                      bulk-parameters:
                                        refresh: "wait_for"
                                      fields:
                                        - name: "content"
                                          expression: "value.content"
                                        - name: "embeddings"
                                          expression: "value.embeddings"
                                """,
                        "pipeline-read.yaml",
                        """
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "result-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - id: read
                                    name: "read"
                                    type: "query-vector-db"
                                    input: "input-topic"
                                    output: "result-topic"
                                    configuration:
                                      datasource: "ESDatasource"
                                      query: |
                                        {
                                            "index": [?],
                                            "knn": {
                                              "field": "embeddings",
                                              "query_vector": ?,
                                              "k": 1,
                                              "num_candidates": 100
                                            }
                                        }
                                      fields:
                                        - "properties.index_name"
                                        - "value.embeddings"
                                      output-field: "value.query-result"
                                """);

        String[] expectedAgents = new String[] {"app-write", "app-read"};
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        "tenant", "app", application, buildInstanceYaml(), expectedAgents)) {
            try (TopicProducer producer = createProducer("insert-topic");
                    TopicProducer input = createProducer("input-topic");
                    TopicConsumer consumer = createConsumer("result-topic")) {

                for (int i = 0; i < 9; i++) {
                    sendFullMessage(
                            producer,
                            "key" + i,
                            "{\"content\": \"hello" + i + "\", \"embeddings\":[999,999," + i + "]}",
                            List.of(SimpleRecord.SimpleHeader.of("index_name", "my-index-000")));
                }
                sendFullMessage(
                        producer,
                        "key9",
                        "{\"content\": \"hello9\", \"embeddings\":[1,1,1]}",
                        List.of(SimpleRecord.SimpleHeader.of("index_name", "my-index-000")));
                executeAgentRunners(applicationRuntime, 15);
                sendMessageWithHeaders(
                        input,
                        "{\"embeddings\":[1,1,2]}",
                        List.of(SimpleRecord.SimpleHeader.of("index_name", "my-index-000")));
                executeAgentRunners(applicationRuntime);

                waitForMessages(
                        consumer,
                        List.of(
                                "{\"embeddings\":[1,1,2],\"query-result\":[{\"content\":\"hello9\",\"embeddings\":[1,1,1],\"id\":\"key9\",\"index\":\"my-index-000\",\"similarity\":0.93994164}]}"));
            }
        }
    }
}
