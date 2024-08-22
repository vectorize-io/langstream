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

import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.mockagents.MockAssetManagerCodeProvider;
import ai.langstream.testrunners.AbstractGenericStreamingApplicationRunner;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class DeployAssetsIT extends AbstractGenericStreamingApplicationRunner {
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
        String eventsTopic = "events-topic" + UUID.randomUUID();
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
                                    events-topic: "%s"
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
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "identity"
                                    id: "step1"
                                    type: "identity"
                                    input: "input-topic"
                                    output: "output-topic"
                                """
                                .formatted(eventsTopic, eventsTopic));
        try (TopicConsumer consumer = createConsumer(eventsTopic);
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
                                            "{\"tenant\":\"tenant\",\"applicationId\":\"app\",\"asset\":{\"id\":\"my-table\",\"name\":\"my-table\",\"config\":{\"datasource\":{\"configuration\":{\"service\":\"jdbc\",\"driverClass\":\"org.postgresql.Driver\",\"url\":\"bar\"}},\"table\":\"my-table\"},\"creation-mode\":\"create-if-not-exists\",\"deletion-mode\":\"delete\",\"asset-type\":\"mock-database-resource\",\"events-topic\":\"%s\"}}"
                                                    .formatted(eventsTopic),
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
                                            "{\"tenant\":\"tenant\",\"applicationId\":\"app\",\"asset\":{\"id\":\"my-table\",\"name\":\"my-table\",\"config\":{\"datasource\":{\"configuration\":{\"service\":\"jdbc\",\"driverClass\":\"org.postgresql.Driver\",\"url\":\"bar\"}},\"table\":\"my-table\"},\"creation-mode\":\"create-if-not-exists\",\"deletion-mode\":\"delete\",\"asset-type\":\"mock-database-resource\",\"events-topic\":\"%s\"}}"
                                                    .formatted(eventsTopic),
                                            mapper.writeValueAsString(read.get("source")));
                                    assertNotNull(read.get("timestamp"));
                                }
                            }));
        }
    }

    @Test
    public void testDeployAssetFailed() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        String secrets =
                """
                        secrets:
                          - id: "the-secret"
                            data:
                               password: "bar"
                        """;
        String eventsTopic = "events-topic" + UUID.randomUUID();
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
                                    events-topic: "%s"
                                    deletion-mode: delete
                                    config:
                                        fail: true
                                        datasource: "the-resource"
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "identity"
                                    id: "step1"
                                    type: "identity"
                                    input: "input-topic"
                                    output: "output-topic"
                                """
                                .formatted(eventsTopic, eventsTopic));
        try (TopicConsumer consumer = createConsumer(eventsTopic); ) {

            try {
                deployApplicationWithSecrets(
                        tenant, "app", application, buildInstanceYaml(), secrets, expectedAgents);
                fail();
            } catch (Throwable e) {
                assertTrue(e.getMessage().contains("Mock failure to deploy asset"));
            }
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
                                    assertEquals("AssetCreationFailed", read.get("type"));
                                    assertEquals("Asset", read.get("category"));
                                    assertEquals(
                                            "{\"tenant\":\"tenant\",\"applicationId\":\"app\",\"asset\":{\"id\":\"my-table\",\"name\":\"my-table\",\"config\":{\"fail\":true,\"datasource\":{\"configuration\":{\"service\":\"jdbc\",\"driverClass\":\"org.postgresql.Driver\",\"url\":\"bar\"}}},\"creation-mode\":\"create-if-not-exists\",\"deletion-mode\":\"delete\",\"asset-type\":\"mock-database-resource\",\"events-topic\":\"%s\"}}"
                                                    .formatted(eventsTopic),
                                            mapper.writeValueAsString(read.get("source")));
                                    assertEquals(
                                            "Mock failure to deploy asset",
                                            ((Map<String, Object>) read.get("data"))
                                                            .get("error-message")
                                                    + "");
                                    assertNotNull(
                                            ((Map<String, Object>) read.get("data"))
                                                    .get("error-stacktrace"));
                                    assertNotNull(read.get("timestamp"));
                                }
                            }));
        }
    }

    @Override
    @SneakyThrows
    protected String buildInstanceYaml() {
        return """
                instance:
                  globals:
                     table-name: "my-table"
                  streamingCluster: %s
                  computeCluster:
                     type: "kubernetes"
                """
                .formatted(OBJECT_MAPPER.writeValueAsString(getStreamingCluster()));
    }
}
