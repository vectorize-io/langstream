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
package ai.langstream.kafka;

import static com.github.tomakehurst.wiremock.client.WireMock.post;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Slf4j
@Disabled
class PineconeIT extends AbstractKafkaApplicationRunner {


    static final String API_KEY = "xx";
    static final String INDEX_NAME = "test" + UUID.randomUUID();

    @Test
    public void testQueryPinecone() throws Exception {
        Map<String, String> application =
                Map.of(
                        "configuration.yaml",
                        """
                                configuration:
                                  resources:
                                    - type: "vector-database"
                                      name: "PineconeDatasource"
                                      configuration:
                                        service: "pinecone"
                                        api-key: "%s"
                                        index-name: "%s"
                                        environment: "unused"
                                        project-name: "unused"
                                        server-side-timeout-sec: 1
                                        connection-timeout-seconds: 60
                                """
                                .formatted(API_KEY, INDEX_NAME),
                        "pipeline-write.yaml",
                        """
                                assets:
                                  - name: "p-index"
                                    asset-type: "pinecone-index"
                                    creation-mode: create-if-not-exists
                                    deletion-mode: delete
                                    config:
                                       datasource: "PineconeDatasource"
                                       dimension: 3
                                       metric: "cosine"
                                       cloud: aws
                                       region: us-east-1
                                topics:
                                  - name: "insert-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - id: write
                                    name: "Write"
                                    type: "vector-db-sink"
                                    input: "insert-topic"
                                    configuration:
                                      datasource: "PineconeDatasource"
                                      vector.id: "value.id"
                                      vector.vector: "value.embeddings"
                                      vector.metadata.content: "value.content"
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
                                    name: "Execute Query"
                                    type: "query-vector-db"
                                    input: "input-topic"
                                    output: "result-topic"
                                    configuration:
                                      datasource: "PineconeDatasource"
                                      query: |
                                        {
                                              "vector": ?,
                                              "topK": 5,
                                              "filter": { "content": {"$eq": ?} }
                                         }
                                      fields:
                                        - "value.embeddings"
                                        - "value.content_query"
                                      output-field: "value.query-result"
                                """);

        String[] expectedAgents = new String[]{"app-write", "app-read"};
        try (ApplicationRuntime applicationRuntime =
                     deployApplication(
                             "tenant", "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                 KafkaConsumer<String, String> consumer = createConsumer("result-topic")) {

                for (int i = 0; i < 10; i++) {
                    sendMessage(
                            "insert-topic",
                            "key" + i,
                            "{\"id\":" + i + ",\"content\": \"hello" + i + "\", \"embeddings\":[999,999," + i + "]}",
                            List.of(),
                            producer);
                }
                executeAgentRunners(applicationRuntime);
                log.info("Waiting for pinecone to index");
                Thread.sleep(10000);

                sendMessage("input-topic", "{\"embeddings\":[999,999,5],\"content_query\":\"hello0\"}", producer);
                executeAgentRunners(applicationRuntime);
                waitForMessages(
                        consumer,
                        List.of(
                                "{\"embeddings\":[999,999,5],\"content_query\":\"hello0\",\"query-result\":[{\"similarity\":0.99999374,\"id\":\"0\",\"content\":\"hello0\"}]}"));
            }
        }
    }
}
