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

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.util.ObjectMapperFactory;
import ai.langstream.testrunners.AbstractGenericStreamingApplicationRunner;
import ai.langstream.utils.HerdDBExtension;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class RerankAgentRunnerIT extends AbstractGenericStreamingApplicationRunner {

    @RegisterExtension static HerdDBExtension herdDB = new HerdDBExtension();

    @SneakyThrows
    private static void validateResults(String message) {
        log.info("Validating message: {}", message);
        Map<String, Object> parsed =
                ObjectMapperFactory.getDefaultMapper().readValue(message, new TypeReference<Map<String, Object>>() {});

        List<Map<String, Object>> relatedDocuments =
                (List<Map<String, Object>>) parsed.get("related_documents");
        assertEquals(relatedDocuments.size(), 8);
        int i = 0;
        assertEquals(
                relatedDocuments.get(i++),
                Map.of("text", "text1", "embeddings_vector", List.of(1.0, 2.0, 3.0, 4.0, 5.0)));
        assertEquals(
                relatedDocuments.get(i++),
                Map.of("text", "text9", "embeddings_vector", List.of(9.0, 2.0, 3.0, 4.0, 5.0)));
        assertEquals(
                relatedDocuments.get(i++),
                Map.of("text", "text0", "embeddings_vector", List.of(0.0, 2.0, 3.0, 4.0, 5.0)));
        assertEquals(
                relatedDocuments.get(i++),
                Map.of("text", "text8", "embeddings_vector", List.of(8.0, 2.0, 3.0, 4.0, 5.0)));
        assertEquals(
                relatedDocuments.get(i++),
                Map.of("text", "text7", "embeddings_vector", List.of(7.0, 2.0, 3.0, 4.0, 5.0)));
        assertEquals(
                relatedDocuments.get(i++),
                Map.of("text", "text2", "embeddings_vector", List.of(2.0, 2.0, 3.0, 4.0, 5.0)));
        assertEquals(
                relatedDocuments.get(i++),
                Map.of("text", "text6", "embeddings_vector", List.of(6.0, 2.0, 3.0, 4.0, 5.0)));
        assertEquals(
                relatedDocuments.get(i++),
                Map.of("text", "text3", "embeddings_vector", List.of(3.0, 2.0, 3.0, 4.0, 5.0)));

        assertEquals("this is a question", parsed.get("question"));
        assertEquals(List.of(1.0, 2.0, 3.0, 4.0, 5.0), parsed.get("question_embeddings"));
    }

    @Test
    public void testSimpleRerank() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-writer", "app-reader"};
        String jdbcUrl = herdDB.getJDBCUrl();

        Map<String, String> application =
                Map.of(
                        "configuration.yaml",
                        """
                                configuration:
                                  resources:
                                    - type: "datasource"
                                      name: "JdbcDatasource"
                                      configuration:
                                        service: "jdbc"
                                        driverClass: "herddb.jdbc.Driver"
                                        url: "%s"
                                        user: "sa"
                                        password: "hdb"
                                        """
                                .formatted(jdbcUrl),
                        "pipeline-writer.yaml",
                        """
                                assets:
                                  - name: "documents-table"
                                    asset-type: "jdbc-table"
                                    creation-mode: create-if-not-exists
                                    config:
                                      table-name: "documents"
                                      datasource: "JdbcDatasource"
                                      create-statements:
                                        - |
                                          CREATE TABLE documents (
                                          filename TEXT,
                                          chunk_id int,
                                          num_tokens int,
                                          lang TEXT,
                                          text TEXT,
                                          embeddings_vector FLOATA,
                                          PRIMARY KEY (filename, chunk_id));
                                topics:
                                  - name: "insert-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "Write"
                                    id: writer
                                    type: "vector-db-sink"
                                    input: insert-topic
                                    configuration:
                                      datasource: "JdbcDatasource"
                                      table-name: "documents"
                                      fields:
                                        - name: "filename"
                                          expression: "value.filename"
                                          primary-key: true
                                        - name: "chunk_id"
                                          expression: "value.chunk_id"
                                          primary-key: true
                                        - name: "embeddings_vector"
                                          expression: "fn:toListOfFloat(value.embeddings_vector)"
                                        - name: "lang"
                                          expression: "value.language"
                                        - name: "text"
                                          expression: "value.text"
                                        - name: "num_tokens"
                                          expression: "value.chunk_num_tokens"
                                """,
                        "pipeline-reader.yaml",
                        """
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                   - name: "convert-to-structure"
                                     id: "reader"
                                     type: "document-to-json"
                                     input: "input-topic"
                                     configuration:
                                       text-field: "question"
                                   - name: "mock-compute-embeddings"
                                     type: "compute"
                                     configuration:
                                       fields:
                                          - name: "value.question_embeddings"
                                            expression: "fn:toListOfFloat([1,2,3,4,5])"
                                   - name: "lookup-related-documents"
                                     type: "query-vector-db"
                                     configuration:
                                       datasource: "JdbcDatasource"
                                       query: "SELECT text,embeddings_vector FROM documents ORDER BY cosine_similarity(embeddings_vector, CAST(? as FLOAT ARRAY)) DESC LIMIT 20"
                                       fields:
                                         - "value.question_embeddings"
                                       output-field: "value.related_documents"
                                   - name: "re-rank documents with MMR"
                                     type: "re-rank"
                                     output: output-topic
                                     configuration:
                                       max: 8
                                       field: "value.related_documents"
                                       query-text: "value.question"
                                       query-embeddings: "value.question_embeddings"
                                       output-field: "value.related_documents"
                                       text-field: "record.text"
                                       embeddings-field: "record.embeddings_vector"
                                       algorithm: "MMR"
                                       lambda: 0.5
                                       k1: 1.5
                                       b: 0.7
                                """);

        // write some data
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (TopicProducer producer = createProducer("insert-topic");
                    TopicProducer input = createProducer("input-topic");
                    TopicConsumer consumer = createConsumer("output-topic")) {

                for (int i = 0; i < 10; i++) {
                    sendMessage(
                            producer,
                            """
                                    {
                                         "filename": "doc%s.pdf",
                                            "chunk_id": 1,
                                            "embeddings_vector": [%s,2,3,4,5],
                                            "language": "en",
                                            "text": "text%s",
                                            "chunk_num_tokens": 10
                                    }
                                    """
                                    .formatted(i, i, i));
                }
                executeAgentRunners(applicationRuntime, 10);

                Consumer<String> validateMessage = RerankAgentRunnerIT::validateResults;

                sendMessage(input, "this is a question");
                executeAgentRunners(applicationRuntime);
                waitForMessages(consumer, List.of(validateMessage));
            }
        }
    }
}
