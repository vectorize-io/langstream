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
package ai.langstream.agents.azureblobstorage;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.langstream.api.runner.code.*;
import ai.langstream.api.runner.code.Record;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import java.nio.charset.StandardCharsets;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@Slf4j
class AzureBlobStorageSourceTest {

    public static final AgentCodeRegistry AGENT_CODE_REGISTRY = new AgentCodeRegistry();
    static Map<String, Object> baseConfig;

    @Container
    private static final GenericContainer<?> azurite =
            new GenericContainer<>(
                            DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite:latest"))
                    .withExposedPorts(10000)
                    .withCommand("azurite-blob --blobHost 0.0.0.0")
                    .withLogConsumer(
                            outputFrame -> log.info("azurite> {}", outputFrame.getUtf8String()));

    static Map<String, Object> configWithNewContainer() {
        String container = "ls-" + UUID.randomUUID();
        Map<String, Object> config = new HashMap<>(baseConfig);
        config.put("container", container);
        return config;
    }

    @BeforeEach
    public void beforeEach() {
        baseConfig =
                Map.of(
                        "endpoint",
                                "http://0.0.0.0:"
                                        + azurite.getMappedPort(10000)
                                        + "/devstoreaccount1",
                        // default credentials for azurite
                        // https://github.com/Azure/Azurite?tab=readme-ov-file#default-storage-account
                        "storage-account-name", "devstoreaccount1",
                        "storage-account-key",
                                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");
    }

    @Test
    void test() throws Exception {
        Map<String, Object> config = configWithNewContainer();
        try (AgentSource source = buildAgentSource(config); ) {
            BlobContainerClient containerClient =
                    AzureBlobStorageSource.createContainerClient(config);
            containerClient.getBlobClient("test.txt").deleteIfExists();
            put(containerClient, "test.txt", "test");
            final List<Record> read = source.read();
            assertEquals(1, read.size());
            assertEquals("test", new String((byte[]) read.get(0).value()));
        }
    }

    @Test
    void testRead() throws Exception {
        Map<String, Object> config = configWithNewContainer();
        try (AgentSource agentSource = buildAgentSource(config); ) {
            BlobContainerClient containerClient =
                    AzureBlobStorageSource.createContainerClient(config);
            String content = "test-content-";
            for (int i = 0; i < 10; i++) {
                String s = content + i;
                put(containerClient, "test-" + i + ".txt", s);
            }

            List<Record> read = agentSource.read();
            assertEquals(1, read.size());
            assertArrayEquals(
                    "test-content-0".getBytes(StandardCharsets.UTF_8),
                    (byte[]) read.get(0).value());

            // DO NOT COMMIT, the source should not return the same objects

            List<Record> read2 = agentSource.read();

            assertEquals(1, read2.size());
            assertArrayEquals(
                    "test-content-1".getBytes(StandardCharsets.UTF_8),
                    (byte[]) read2.get(0).value());

            // COMMIT (out of order)
            agentSource.commit(read2);
            agentSource.commit(read);

            Iterator<BlobItem> results = containerClient.listBlobs().stream().iterator();
            for (int i = 2; i < 10; i++) {
                BlobItem item = results.next();
                assertEquals("test-" + i + ".txt", item.getName());
            }

            List<Record> all = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                all.addAll(agentSource.read());
            }

            agentSource.commit(all);
            all.clear();

            results = containerClient.listBlobs().stream().iterator();
            assertFalse(results.hasNext());

            for (int i = 0; i < 10; i++) {
                all.addAll(agentSource.read());
            }
            agentSource.commit(all);
            agentSource.commit(List.of());
        }
    }

    @Test
    void emptyBucket() throws Exception {
        Map<String, Object> config = configWithNewContainer();
        try (AgentSource agentSource = buildAgentSource(config); ) {
            BlobContainerClient containerClient =
                    AzureBlobStorageSource.createContainerClient(config);
            assertFalse(containerClient.listBlobs().stream().iterator().hasNext());
            agentSource.commit(List.of());
            List<Record> read = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                read.addAll(agentSource.read());
            }
            assertFalse(containerClient.listBlobs().stream().iterator().hasNext());
            agentSource.commit(read);
        }
    }

    @Test
    void commitNonExistent() throws Exception {
        Map<String, Object> config = configWithNewContainer();
        try (AgentSource agentSource = buildAgentSource(config); ) {
            BlobContainerClient containerClient =
                    AzureBlobStorageSource.createContainerClient(config);
            String content = "test-content";
            put(containerClient, "test.txt", content);
            List<Record> read = agentSource.read();

            containerClient.getBlobClient("test.txt").deleteIfExists();
            agentSource.commit(read);
        }
    }

    @Test
    void testReadRecursive() throws Exception {
        Map<String, Object> config = configWithNewContainer();

        try (AgentSource agentSource = buildAgentSource(config); ) {
            BlobContainerClient containerClient =
                    AzureBlobStorageSource.createContainerClient(config);
            put(containerClient, "root.txt", "root");
            put(containerClient, "dir1/item.txt", "item");
            put(containerClient, "dir1/dir2/item2.txt", "item2");
            List<Record> all = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                all.addAll(agentSource.read());
            }
            assertEquals(1, all.size());
            assertEquals("root", new String((byte[]) all.get(0).value(), StandardCharsets.UTF_8));
        }
        config.put("recursive", "true");

        try (AgentSource agentSource = buildAgentSource(config); ) {
            List<Record> all = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                all.addAll(agentSource.read());
            }
            assertEquals(3, all.size());
            for (Record record : all) {
                String name = record.getHeader("name").valueAsString();
                switch (name) {
                    case "root.txt":
                        assertEquals(
                                "root",
                                new String((byte[]) record.value(), StandardCharsets.UTF_8));
                        break;
                    case "dir1/item.txt":
                        assertEquals(
                                "item",
                                new String((byte[]) record.value(), StandardCharsets.UTF_8));
                        break;
                    case "dir1/dir2/item2.txt":
                        assertEquals(
                                "item2",
                                new String((byte[]) record.value(), StandardCharsets.UTF_8));
                        break;
                    default:
                        fail("Unexpected record: " + name);
                }
            }
        }
    }

    @Test
    void testReadPrefix() throws Exception {
        Map<String, Object> config = configWithNewContainer();
        config.put("path-prefix", "dir1/");

        try (AgentSource agentSource = buildAgentSource(config); ) {
            BlobContainerClient containerClient =
                    AzureBlobStorageSource.createContainerClient(config);
            put(containerClient, "root.txt", "root");
            put(containerClient, "dir1/item.txt", "item");
            put(containerClient, "dir1/dir2/item2.txt", "item2");
            List<Record> all = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                all.addAll(agentSource.read());
            }
            assertEquals(1, all.size());
            assertEquals("item", new String((byte[]) all.get(0).value(), StandardCharsets.UTF_8));
        }
        config.put("recursive", "true");

        try (AgentSource agentSource = buildAgentSource(config); ) {
            List<Record> all = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                all.addAll(agentSource.read());
            }
            assertEquals(2, all.size());
            for (Record record : all) {
                String name = record.getHeader("name").valueAsString();
                switch (name) {
                    case "dir1/item.txt":
                        assertEquals(
                                "item",
                                new String((byte[]) record.value(), StandardCharsets.UTF_8));
                        break;
                    case "dir1/dir2/item2.txt":
                        assertEquals(
                                "item2",
                                new String((byte[]) record.value(), StandardCharsets.UTF_8));
                        break;
                    default:
                        fail("Unexpected record: " + name);
                }
            }
        }
    }

    private static void put(BlobContainerClient containerClient, String name, String content) {
        containerClient.getBlobClient(name).upload(BinaryData.fromString(content));
    }

    private AgentSource buildAgentSource(Map<String, Object> config) throws Exception {
        AgentSource agentSource =
                (AgentSource)
                        AGENT_CODE_REGISTRY.getAgentCode("azure-blob-storage-source").agentCode();
        Map<String, Object> configs = new HashMap<>(config);
        configs.put("idle-time", 1);
        agentSource.init(configs);
        agentSource.setMetadata(
                "my-agent-id", "azure-blob-storage-source", System.currentTimeMillis());
        AgentContext context = mock(AgentContext.class);
        when(context.getMetricsReporter()).thenReturn(MetricsReporter.DISABLED);
        when(context.getGlobalAgentId()).thenReturn("global-agent-id");
        when(context.getTenant()).thenReturn("my-tenant");
        agentSource.setContext(context);
        agentSource.start();
        return agentSource;
    }
}
