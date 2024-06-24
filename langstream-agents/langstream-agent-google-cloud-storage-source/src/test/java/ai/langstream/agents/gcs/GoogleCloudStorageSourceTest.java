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
package ai.langstream.agents.gcs;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.langstream.api.runner.code.*;
import ai.langstream.api.runner.code.Record;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@Slf4j
class GoogleCloudStorageSourceTest {

    public static final AgentCodeRegistry AGENT_CODE_REGISTRY = new AgentCodeRegistry();

    @Container
    private static final GenericContainer<?> gcsServer =
            new GenericContainer<>(DockerImageName.parse("fsouza/fake-gcs-server:latest"))
                    .withExposedPorts(4443)
                    .withCreateContainerCmdModifier(
                            it -> it.withEntrypoint("/bin/fake-gcs-server", "-scheme", "http"))
                    .withLogConsumer(
                            outputFrame -> log.info("gcs> {}", outputFrame.getUtf8String()));

    static Map<String, Object> configWithNewBucket() {
        String bucketName = "ls-" + UUID.randomUUID();
        Map<String, Object> config = new HashMap<>();
        config.put("bucket-name", bucketName);
        return config;
    }

    @Test
    void test() throws Exception {
        Map<String, Object> config = configWithNewBucket();

        try (AgentSource source = buildAgentSource(config);
                Storage storage = getClient().getService(); ) {
            BlobInfo blobInfo = getBlobInfo(config, "test.txt");
            storage.create(blobInfo, "test".getBytes(StandardCharsets.UTF_8));
            final List<Record> read = source.read();
            assertEquals(1, read.size());
            assertEquals("test", new String((byte[]) read.get(0).value()));
        }
    }

    private static BlobInfo getBlobInfo(Map<String, Object> config, String name) {
        BlobInfo blobInfo =
                BlobInfo.newBuilder(BlobId.of((String) config.get("bucket-name"), name)).build();
        return blobInfo;
    }

    @Test
    void testRead() throws Exception {
        Map<String, Object> config = configWithNewBucket();
        try (AgentSource agentSource = buildAgentSource(config);
                Storage storage = getClient().getService(); ) {
            String content = "test-content-";
            for (int i = 0; i < 10; i++) {
                String s = content + i;
                storage.create(
                        getBlobInfo(config, "test-" + i + ".txt"),
                        s.getBytes(StandardCharsets.UTF_8));
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

            Iterator<Blob> iterator =
                    storage.list((String) config.get("bucket-name")).getValues().iterator();
            for (int i = 2; i < 10; i++) {
                Blob next = iterator.next();
                assertEquals("test-" + i + ".txt", next.getName());
            }

            List<Record> all = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                all.addAll(agentSource.read());
            }

            agentSource.commit(all);
            all.clear();

            iterator = storage.list((String) config.get("bucket-name")).getValues().iterator();
            assertFalse(iterator.hasNext());

            for (int i = 0; i < 10; i++) {
                all.addAll(agentSource.read());
            }
            agentSource.commit(all);
            agentSource.commit(List.of());
        }
    }

    @Test
    void emptyBucket() throws Exception {
        Map<String, Object> config = configWithNewBucket();
        try (AgentSource agentSource = buildAgentSource(config);
                Storage storage = getClient().getService(); ) {
            assertFalse(
                    storage.list((String) config.get("bucket-name"))
                            .iterateAll()
                            .iterator()
                            .hasNext());
            agentSource.commit(List.of());
            List<Record> read = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                read.addAll(agentSource.read());
            }
            assertFalse(
                    storage.list((String) config.get("bucket-name"))
                            .iterateAll()
                            .iterator()
                            .hasNext());
            agentSource.commit(read);
        }
    }

    @Test
    void commitNonExistent() throws Exception {
        Map<String, Object> config = configWithNewBucket();
        try (AgentSource agentSource = buildAgentSource(config);
                Storage storage = getClient().getService(); ) {
            String content = "test-content";
            storage.create(
                    getBlobInfo(config, "test.txt"), content.getBytes(StandardCharsets.UTF_8));
            List<Record> read = agentSource.read();
            storage.delete((String) config.get("bucket-name"), "test.txt");
            agentSource.commit(read);
        }
    }

    private AgentSource buildAgentSource(Map<String, Object> config) throws Exception {
        AgentSource agentSource =
                (AgentSource)
                        AGENT_CODE_REGISTRY.getAgentCode("google-cloud-storage-source").agentCode();
        assertTrue(agentSource instanceof GoogleCloudStorageSource);
        agentSource =
                new GoogleCloudStorageSource() {
                    @Override
                    StorageOptions initStorageOptions(String serviceAccountJson)
                            throws IOException {
                        return getClient();
                    }
                };

        Map<String, Object> configs = new HashMap<>(config);
        configs.put("service-account-json", "ignore");
        configs.put("idle-time", 1);
        agentSource.init(configs);
        agentSource.setMetadata(
                "my-agent-id", "google-cloud-storage-source", System.currentTimeMillis());
        AgentContext context = mock(AgentContext.class);
        when(context.getMetricsReporter()).thenReturn(MetricsReporter.DISABLED);
        when(context.getGlobalAgentId()).thenReturn("global-agent-id");
        when(context.getTenant()).thenReturn("my-tenant");
        agentSource.setContext(context);
        agentSource.start();
        return agentSource;
    }

    private static StorageOptions getClient() {
        return StorageOptions.newBuilder()
                .setCredentials(NoCredentials.getInstance())
                .setHost("http://localhost:" + gcsServer.getMappedPort(4443))
                .setProjectId("fake-project")
                .build();
    }
}
