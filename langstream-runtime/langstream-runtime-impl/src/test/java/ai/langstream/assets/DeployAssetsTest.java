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
package ai.langstream.assets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.kafka.AbstractKafkaApplicationRunner;
import ai.langstream.mockagents.MockAssetManagerCodeProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;

@Slf4j
class DeployAssetsTest extends AbstractKafkaApplicationRunner {

    @Test
    public void testDeployAsset() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        String secrets =
                """
                            secrets:
                              - id: "the-secret"
                                data:
                                   password: "bar"
                            """;
        Map<String, String> application =
                Map.of(
                        "configuration.yaml",
                        """
                            configuration:
                               resources:
                                    - type: "datasource"
                                      name: "the-resource"
                                      configuration:
                                         service: jdbc
                                         url: "${secrets.the-secret.password}"
                                         driverClass: "org.postgresql.Driver"
                            """,
                        "module.yaml",
                        """
                        module: "module-1"
                        id: "pipeline-1"
                        assets:
                          - name: "my-table"
                            creation-mode: create-if-not-exists
                            asset-type: "mock-database-resource"
                            events-topic: "events-topic"
                            deletion-mode: delete
                            config:
                                table: "${globals.table-name}"
                                datasource: "the-resource"
                          - name: "my-table2"
                            creation-mode: create-if-not-exists
                            asset-type: "mock-database-resource"
                            config:
                                table: "other2"
                                datasource: "the-resource"
                        topics:
                          - name: "input-topic"
                            creation-mode: create-if-not-exists
                          - name: "output-topic"
                            creation-mode: create-if-not-exists
                          - name: "events-topic"
                            creation-mode: create-if-not-exists
                        pipeline:
                          - name: "identity"
                            id: "step1"
                            type: "identity"
                            input: "input-topic"
                            output: "output-topic"
                        """);
        try (KafkaConsumer<String, String> consumer = createConsumer("events-topic");
                ApplicationRuntime applicationRuntime =
                        deployApplicationWithSecrets(
                                tenant,
                                "app",
                                application,
                                buildInstanceYaml(),
                                secrets,
                                expectedAgents)) {
            CopyOnWriteArrayList<AssetDefinition> deployedAssets =
                    MockAssetManagerCodeProvider.MockDatabaseResourceAssetManager.DEPLOYED_ASSETS;
            assertEquals(2, deployedAssets.size());
            AssetDefinition deployedAsset = deployedAssets.get(0);
            assertEquals("my-table", deployedAsset.getConfig().get("table"));
            Map<String, Object> datasource =
                    (Map<String, Object>) deployedAsset.getConfig().get("datasource");
            Map<String, Object> datasourceConfiguration =
                    (Map<String, Object>) datasource.get("configuration");
            assertEquals("bar", datasourceConfiguration.get("url"));

            waitForMessages(
                    consumer,
                    List.of(
                            new Consumer<>() {
                                @Override
                                @SneakyThrows
                                public void accept(Object o) {
                                    log.info("Received: {}", o);
                                    ObjectMapper mapper = new ObjectMapper();
                                    Map read = mapper.readValue((String) o, Map.class);
                                    assertEquals("AssetCreated", read.get("type"));
                                    assertEquals("Asset", read.get("category"));
                                    assertEquals(
                                            "{\"tenant\":\"tenant\",\"applicationId\":\"app\",\"asset\":{\"id\":\"my-table\",\"name\":\"my-table\",\"config\":{\"datasource\":{\"configuration\":{\"service\":\"jdbc\",\"driverClass\":\"org.postgresql.Driver\",\"url\":\"bar\"}},\"table\":\"my-table\"},\"creation-mode\":\"create-if-not-exists\",\"deletion-mode\":\"delete\",\"asset-type\":\"mock-database-resource\",\"events-topic\":\"events-topic\"}}",
                                            mapper.writeValueAsString(read.get("source")));
                                    assertNotNull(read.get("timestamp"));
                                }
                            }));

            final ExecutionPlan plan = applicationRuntime.implementation();
            applicationDeployer.cleanup(tenant, plan, codeDirectory);
            assertEquals(1, deployedAssets.size());

            waitForMessages(
                    consumer,
                    List.of(
                            new Consumer<>() {
                                @Override
                                @SneakyThrows
                                public void accept(Object o) {
                                    log.info("Received: {}", o);
                                    ObjectMapper mapper = new ObjectMapper();
                                    Map read = mapper.readValue((String) o, Map.class);
                                    assertEquals("AssetDeleted", read.get("type"));
                                    assertEquals("Asset", read.get("category"));
                                    assertEquals(
                                            "{\"tenant\":\"tenant\",\"applicationId\":\"app\",\"asset\":{\"id\":\"my-table\",\"name\":\"my-table\",\"config\":{\"datasource\":{\"configuration\":{\"service\":\"jdbc\",\"driverClass\":\"org.postgresql.Driver\",\"url\":\"bar\"}},\"table\":\"my-table\"},\"creation-mode\":\"create-if-not-exists\",\"deletion-mode\":\"delete\",\"asset-type\":\"mock-database-resource\",\"events-topic\":\"events-topic\"}}",
                                            mapper.writeValueAsString(read.get("source")));
                                    assertNotNull(read.get("timestamp"));
                                }
                            }));
        }
    }

    @Override
    protected String buildInstanceYaml() {
        return """
                instance:
                  globals:
                     table-name: "my-table"
                  streamingCluster:
                    type: "kafka"
                    configuration:
                      admin:
                        bootstrap.servers: "%s"
                  computeCluster:
                     type: "kubernetes"
                """
                .formatted(kafkaContainer.getBootstrapServers());
    }
}
