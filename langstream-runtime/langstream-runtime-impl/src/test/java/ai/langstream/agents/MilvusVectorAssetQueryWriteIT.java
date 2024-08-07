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

import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.testrunners.AbstractGenericStreamingApplicationRunner;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Slf4j
class MilvusVectorAssetQueryWriteIT extends AbstractGenericStreamingApplicationRunner {

    @Test
    @Disabled() // "This test requires a running Milvus instance"
    public void testMilvus() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        String configuration =
                """
                        configuration:
                          resources:
                            - type: "vector-database"
                              name: "MilvusDatasource"
                              configuration:
                                service: "milvus"
                                host: "localhost"
                                port: 19530
                        """;

        Map<String, String> applicationWriter =
                Map.of(
                        "configuration.yaml",
                        configuration,
                        "pipeline.yaml",
                        """
                                assets:
                                  - name: "documents-table"
                                    asset-type: "milvus-collection"
                                    creation-mode: create-if-not-exists
                                    config:
                                       collection-name: "documents"
                                       database-name: "default"
                                       datasource: "MilvusDatasource"
                                       create-statements:
                                          - |
                                            {
                                                "collection-name": "documents",
                                                "database-name": "default",
                                                "dimension": 5,
                                                "field-types": [
                                                   {
                                                      "name": "id",
                                                      "primary-key": true,
                                                      "data-type": "Int64"
                                                   },
                                                   {
                                                      "name": "description",
                                                      "data-type": "string"
                                                   },
                                                   {
                                                      "name": "name",
                                                      "data-type": "string"
                                                   }
                                                ]
                                            }
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - id: step1
                                    name: "Write a record to Milvus"
                                    type: "vector-db-sink"
                                    input: "input-topic"
                                    configuration:
                                      datasource: "MilvusDatasource"
                                      collection-name: "documents"
                                      fields:
                                        - name: id
                                          expression: "fn:toLong(value.documentId)"
                                        - name: vector
                                          expression: "fn:toListOfFloat(value.embeddings)"
                                        - name: name
                                          expression: "value.name"
                                        - name: description
                                          expression: "value.description"
                                """);

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", applicationWriter, buildInstanceYaml(), expectedAgents)) {
            try (TopicProducer producer = createProducer("input-topic"); ) {

                sendMessage(
                        producer,
                        "{\"documentId\":1, \"embeddings\":[0.1,0.2,0.3,0.4,0.5],\"name\":\"A name\",\"description\":\"A description\"}");

                executeAgentRunners(applicationRuntime);
            }
        }

        Map<String, String> applicationReader =
                Map.of(
                        "configuration.yaml",
                        configuration,
                        "pipeline.yaml",
                        """
                            topics:
                              - name: "questions-topic"
                                creation-mode: create-if-not-exists
                              - name: "answers-topic"
                                creation-mode: create-if-not-exists
                            pipeline:
                              - id: step1
                                name: "Read a record from Milvus"
                                type: "query-vector-db"
                                input: "questions-topic"
                                output: "answers-topic"
                                configuration:
                                  datasource: "MilvusDatasource"
                                  query: |
                                        {
                                          "collection-name": "documents",
                                          "vectors": ?,
                                          "top-k": 1
                                        }
                                  only-first: true
                                  output-field: "value.queryresult"
                                  fields:
                                     - "value.embeddings"

                            """);

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", applicationReader, buildInstanceYaml(), expectedAgents)) {
            try (TopicProducer producer = createProducer("questions-topic");
                    TopicConsumer consumer = createConsumer("answers-topic")) {

                sendMessage(producer, "{\"embeddings\":[0.1,0.2,0.3,0.4,0.5]}");

                executeAgentRunners(applicationRuntime);

                waitForMessages(
                        consumer,
                        List.of(
                                "{\"embeddings\":[0.1,0.2,0.3,0.4,0.5],\"queryresult\":{\"distance\":\"0.0\",\"id\":\"1\"}}"));
            }
        }
    }
}
