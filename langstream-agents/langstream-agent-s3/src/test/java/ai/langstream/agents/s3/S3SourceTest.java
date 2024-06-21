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
package ai.langstream.agents.s3;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import ai.langstream.ai.agents.commons.state.S3StateStorage;
import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.MetricsReporter;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import io.minio.*;
import io.minio.messages.Item;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class S3SourceTest {

    private static final AgentCodeRegistry AGENT_CODE_REGISTRY = new AgentCodeRegistry();
    private static final DockerImageName localstackImage =
            DockerImageName.parse("localstack/localstack:2.2.0");

    @Container
    private static final LocalStackContainer localstack =
            new LocalStackContainer(localstackImage).withServices(S3);

    private static MinioClient minioClient;

    @BeforeAll
    static void setup() {
        minioClient =
                MinioClient.builder()
                        .endpoint(localstack.getEndpointOverride(S3).toString())
                        .build();
    }

    @Test
    void testProcess() throws Exception {
        // Add some objects to the bucket
        String bucket = "langstream-test-" + UUID.randomUUID();
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
        try (AgentProcessor agentProcessor = buildAgentProcessor(bucket); ) {
            String content = "test-content-";
            for (int i = 0; i < 2; i++) {
                String s = content + i;
                minioClient.putObject(
                        PutObjectArgs.builder().bucket(bucket).object("test-" + i + ".txt").stream(
                                        new ByteArrayInputStream(
                                                s.getBytes(StandardCharsets.UTF_8)),
                                        s.length(),
                                        -1)
                                .build());
            }

            // Create a input record that specifies the first file
            String objectName = "test-0.txt";
            SimpleRecord someRecord =
                    SimpleRecord.builder()
                            .value("{\"objectName\": \"" + objectName + "\"}")
                            .headers(
                                    List.of(
                                            new SimpleRecord.SimpleHeader(
                                                    "original", "Some session id")))
                            .build();

            // Process the record
            List<AgentProcessor.SourceRecordAndResult> resultsForRecord = new ArrayList<>();
            agentProcessor.process(List.of(someRecord), resultsForRecord::add);

            // Should be a record for the file
            assertEquals(1, resultsForRecord.size());

            // the processor must pass downstream the original record
            Record emittedToDownstream = resultsForRecord.get(0).sourceRecord();
            assertSame(emittedToDownstream, someRecord);

            // The resulting record should have the file content as the value
            assertArrayEquals(
                    "test-content-0".getBytes(StandardCharsets.UTF_8),
                    (byte[]) resultsForRecord.get(0).resultRecords().get(0).value());
            // The resulting record should have the file name as the key
            assertEquals(objectName, resultsForRecord.get(0).resultRecords().get(0).key());

            // Check headers
            Collection<Header> headers = resultsForRecord.get(0).resultRecords().get(0).headers();

            // Make sure the name header matches the object name
            Optional<Header> foundNameHeader =
                    headers.stream()
                            .filter(
                                    header ->
                                            "name".equals(header.key())
                                                    && objectName.equals(header.value()))
                            .findFirst();

            assertTrue(
                    foundNameHeader
                            .isPresent()); // Check that the object name is passed in the record

            // Make sure the original header matches the passed in header
            Optional<Header> foundOrigHeader =
                    headers.stream()
                            .filter(
                                    header ->
                                            "original".equals(header.key())
                                                    && "Some session id".equals(header.value()))
                            .findFirst();

            assertTrue(
                    foundOrigHeader
                            .isPresent()); // Check that the object name is passed in the record

            // Get the next file
            String secondObjectName = "test-1.txt";
            someRecord =
                    SimpleRecord.builder()
                            .value("{\"objectName\": \"" + secondObjectName + "\"}")
                            .headers(
                                    List.of(
                                            new SimpleRecord.SimpleHeader(
                                                    "original", "Some session id")))
                            .build();

            resultsForRecord = new ArrayList<>();
            agentProcessor.process(List.of(someRecord), resultsForRecord::add);

            // Make sure the second file is processed
            assertEquals(1, resultsForRecord.size());
        }
    }

    @Test
    void testProcessFromDirectory() throws Exception {
        // Add some objects to the bucket in a directory
        String bucket = "langstream-test-" + UUID.randomUUID();
        String directory = "test-dir/";
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
        try (AgentProcessor agentProcessor = buildAgentProcessor(bucket); ) {
            String content = "test-content-";
            for (int i = 0; i < 2; i++) {
                String s = content + i;
                minioClient.putObject(
                        PutObjectArgs.builder()
                                .bucket(bucket)
                                .object(directory + "test-" + i + ".txt")
                                .stream(
                                        new ByteArrayInputStream(
                                                s.getBytes(StandardCharsets.UTF_8)),
                                        s.length(),
                                        -1)
                                .build());
            }

            // Process the first file in the directory
            String firstObjectName = directory + "test-0.txt";
            SimpleRecord firstRecord =
                    SimpleRecord.builder()
                            .value("{\"objectName\": \"" + firstObjectName + "\"}")
                            .headers(
                                    List.of(
                                            new SimpleRecord.SimpleHeader(
                                                    "original", "Some session id")))
                            .build();

            List<AgentProcessor.SourceRecordAndResult> resultsForFirstRecord = new ArrayList<>();
            agentProcessor.process(List.of(firstRecord), resultsForFirstRecord::add);
            // Make sure the first file is processed and that the original record is passed
            // downstream
            assertEquals(1, resultsForFirstRecord.size());
            assertSame(firstRecord, resultsForFirstRecord.get(0).sourceRecord());
            // Check that the content of the first record is the content of the first file
            assertArrayEquals(
                    "test-content-0".getBytes(StandardCharsets.UTF_8),
                    (byte[]) resultsForFirstRecord.get(0).resultRecords().get(0).value());
            // Check that the key of the first record is the name of the first file
            assertEquals(
                    firstObjectName, resultsForFirstRecord.get(0).resultRecords().get(0).key());

            // Check headers for first record
            // The name header contains the file name
            Collection<Header> firstRecordHeaders =
                    resultsForFirstRecord.get(0).resultRecords().get(0).headers();
            assertTrue(
                    firstRecordHeaders.stream()
                            .anyMatch(
                                    header ->
                                            "name".equals(header.key())
                                                    && firstObjectName.equals(header.value())));
            // The original header contains the original header from the record
            assertTrue(
                    firstRecordHeaders.stream()
                            .anyMatch(
                                    header ->
                                            "original".equals(header.key())
                                                    && "Some session id".equals(header.value())));

            // Process the second file in the directory
            String secondObjectName = directory + "test-1.txt";
            SimpleRecord secondRecord =
                    SimpleRecord.builder()
                            .value("{\"objectName\": \"" + secondObjectName + "\"}")
                            .headers(
                                    List.of(
                                            new SimpleRecord.SimpleHeader(
                                                    "original", "Some session id")))
                            .build();

            List<AgentProcessor.SourceRecordAndResult> resultsForSecondRecord = new ArrayList<>();
            agentProcessor.process(List.of(secondRecord), resultsForSecondRecord::add);

            assertEquals(1, resultsForSecondRecord.size());
            assertSame(secondRecord, resultsForSecondRecord.get(0).sourceRecord());

            assertArrayEquals(
                    "test-content-1".getBytes(StandardCharsets.UTF_8),
                    (byte[]) resultsForSecondRecord.get(0).resultRecords().get(0).value());

            // Check headers for second record
            Collection<Header> secondRecordHeaders =
                    resultsForSecondRecord.get(0).resultRecords().get(0).headers();
            assertTrue(
                    secondRecordHeaders.stream()
                            .anyMatch(
                                    header ->
                                            "name".equals(header.key())
                                                    && secondObjectName.equals(header.value())));
            assertTrue(
                    secondRecordHeaders.stream()
                            .anyMatch(
                                    header ->
                                            "original".equals(header.key())
                                                    && "Some session id".equals(header.value())));
        }
    }

    @Test
    void testRead() throws Exception {
        String bucket = "langstream-test-" + UUID.randomUUID();
        try (AgentSource agentSource = buildAgentSource(bucket); ) {
            String content = "test-content-";
            for (int i = 0; i < 10; i++) {
                String s = content + i;
                minioClient.putObject(
                        PutObjectArgs.builder().bucket(bucket).object("test-" + i + ".txt").stream(
                                        new ByteArrayInputStream(
                                                s.getBytes(StandardCharsets.UTF_8)),
                                        s.length(),
                                        -1)
                                .build());
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

            Iterator<Result<Item>> results =
                    minioClient
                            .listObjects(ListObjectsArgs.builder().bucket(bucket).build())
                            .iterator();
            for (int i = 2; i < 10; i++) {
                Result<Item> item = results.next();
                assertEquals("test-" + i + ".txt", item.get().objectName());
            }

            List<Record> all = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                all.addAll(agentSource.read());
            }

            agentSource.commit(all);
            all.clear();

            results =
                    minioClient
                            .listObjects(ListObjectsArgs.builder().bucket(bucket).build())
                            .iterator();
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
        String bucket = "langstream-test-" + UUID.randomUUID();
        try (AgentSource agentSource = buildAgentSource(bucket); ) {
            assertFalse(
                    minioClient
                            .listObjects(ListObjectsArgs.builder().bucket(bucket).build())
                            .iterator()
                            .hasNext());
            agentSource.commit(List.of());
            List<Record> read = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                read.addAll(agentSource.read());
            }
            assertFalse(
                    minioClient
                            .listObjects(ListObjectsArgs.builder().bucket(bucket).build())
                            .iterator()
                            .hasNext());
            agentSource.commit(read);
        }
    }

    @Test
    void commitNonExistent() throws Exception {
        String bucket = "langstream-test-" + UUID.randomUUID();
        try (AgentSource agentSource = buildAgentSource(bucket); ) {
            String content = "test-content";
            S3StateStorage.putWithRetries(
                    minioClient,
                    () ->
                            PutObjectArgs.builder().bucket(bucket).object("test").stream(
                                            new ByteArrayInputStream(
                                                    content.getBytes(StandardCharsets.UTF_8)),
                                            content.length(),
                                            -1)
                                    .build());
            List<Record> read = agentSource.read();
            minioClient.removeObject(
                    RemoveObjectArgs.builder().bucket(bucket).object("test").build());
            agentSource.commit(read);
        }
    }

    private AgentSource buildAgentSource(String bucket) throws Exception {
        return buildAgentSource(bucket, Map.of());
    }

    private AgentSource buildAgentSource(String bucket, Map<String, Object> additionalConfigs)
            throws Exception {
        AgentSource agentSource =
                (AgentSource) AGENT_CODE_REGISTRY.getAgentCode("s3-source").agentCode();
        String endpoint = localstack.getEndpointOverride(S3).toString();
        Map<String, Object> configs = new HashMap<>(additionalConfigs);
        configs.put("endpoint", endpoint);
        configs.put("bucketName", bucket);
        configs.put("idle-time", 1);
        agentSource.init(configs);
        agentSource.setMetadata("my-agent-id", "s3-source", System.currentTimeMillis());
        AgentContext context = mock(AgentContext.class);
        when(context.getMetricsReporter()).thenReturn(MetricsReporter.DISABLED);
        when(context.getGlobalAgentId()).thenReturn("global-agent-id");
        when(context.getTenant()).thenReturn("my-tenant");
        agentSource.setContext(context);
        agentSource.start();
        return agentSource;
    }

    private AgentProcessor buildAgentProcessor(String bucket) throws Exception {
        AgentProcessor agent =
                (AgentProcessor) AGENT_CODE_REGISTRY.getAgentCode("s3-processor").agentCode();
        Map<String, Object> configs = new HashMap<>();
        String endpoint = localstack.getEndpointOverride(S3).toString();
        configs.put("endpoint", endpoint);
        configs.put("bucketName", bucket);
        configs.put("objectName", "{{ value.objectName }}");
        agent.init(configs);
        agent.setMetadata("my-agent-id", "s3-processor", System.currentTimeMillis());
        AgentContext context = mock(AgentContext.class);
        when(context.getMetricsReporter()).thenReturn(MetricsReporter.DISABLED);
        agent.setContext(context);
        agent.start();
        return agent;
    }

    @Test
    void testIsExtensionAllowed() {
        assertTrue(S3Source.isExtensionAllowed("aaa", Set.of("*")));
        assertTrue(S3Source.isExtensionAllowed("", Set.of("*")));
        assertTrue(S3Source.isExtensionAllowed(".aaa", Set.of("*")));
        assertTrue(S3Source.isExtensionAllowed("aaa.", Set.of("*")));

        assertFalse(S3Source.isExtensionAllowed("aaa", Set.of("aaa")));
        assertFalse(S3Source.isExtensionAllowed("", Set.of("aaa")));
        assertTrue(S3Source.isExtensionAllowed(".aaa", Set.of("aaa")));
        assertFalse(S3Source.isExtensionAllowed("aaa.", Set.of("aaa")));

        assertFalse(S3Source.isExtensionAllowed("aaa", Set.of("bbb")));
        assertFalse(S3Source.isExtensionAllowed("", Set.of("bbb")));
        assertFalse(S3Source.isExtensionAllowed(".aaa", Set.of("bbb")));
        assertFalse(S3Source.isExtensionAllowed("aaa.", Set.of("b")));
    }

    @Test
    void testStateStoreWithNoDeletes() throws Exception {
        String bucket = "langstream-test-" + UUID.randomUUID();
        String stateBucket = "langstream-test-" + UUID.randomUUID();
        try (AgentSource agentSource =
                buildAgentSource(
                        bucket,
                        Map.of(
                                "delete-objects",
                                false,
                                "state-storage",
                                "s3",
                                "state-storage-s3-bucket",
                                stateBucket,
                                "state-storage-s3-endpoint",
                                localstack.getEndpointOverride(S3).toString())); ) {
            String content = "test-content-";
            for (int i = 0; i < 10; i++) {
                String s = content + i;
                minioClient.putObject(
                        PutObjectArgs.builder().bucket(bucket).object("test-" + i + ".txt").stream(
                                        new ByteArrayInputStream(
                                                s.getBytes(StandardCharsets.UTF_8)),
                                        s.length(),
                                        -1)
                                .build());
            }
            for (int i = 0; i < 10; i++) {
                List<Record> read = agentSource.read();
                assertEquals(1, read.size());
                assertArrayEquals(
                        ("test-content-" + i).getBytes(StandardCharsets.UTF_8),
                        (byte[]) read.get(0).value());
                assertEquals("test-" + i + ".txt", read.get(0).getHeader("name").valueAsString());
                assertEquals(bucket, read.get(0).getHeader("bucket").valueAsString());
                assertEquals("new", read.get(0).getHeader("content_diff").valueAsString());
                agentSource.commit(read);
            }

            Iterator<Result<Item>> iterator =
                    minioClient
                            .listObjects(ListObjectsArgs.builder().bucket(bucket).build())
                            .iterator();
            for (int i = 0; i < 10; i++) {
                Result<Item> item = iterator.next();
                assertEquals("test-" + i + ".txt", item.get().objectName());
            }
            assertFalse(iterator.hasNext());
            Item stateObject =
                    minioClient
                            .listObjects(ListObjectsArgs.builder().bucket(stateBucket).build())
                            .iterator()
                            .next()
                            .get();
            assertEquals("global-agent-id.my-agent-id.status.json", stateObject.objectName());
            S3Source.S3SourceState state =
                    ((S3Source) agentSource).getStateStorage().get(S3Source.S3SourceState.class);
            assertEquals(10, state.getAllTimeObjects().size());
            for (int i = 0; i < 10; i++) {
                assertNotNull(state.getAllTimeObjects().get(bucket + "@test-" + i + ".txt"));
            }
            agentSource.read();
            state = ((S3Source) agentSource).getStateStorage().get(S3Source.S3SourceState.class);
            assertEquals(10, state.getAllTimeObjects().size());
            String etag0 = null;
            for (int i = 0; i < 10; i++) {
                assertNotNull(state.getAllTimeObjects().get(bucket + "@test-" + i + ".txt"));
                if (i == 0) {
                    etag0 = state.getAllTimeObjects().get(bucket + "@test-" + i + ".txt");
                }
            }

            final String s = "changed-contnet";
            S3StateStorage.putWithRetries(
                    minioClient,
                    () ->
                            PutObjectArgs.builder().bucket(bucket).object("test-0.txt").stream(
                                            new ByteArrayInputStream(
                                                    s.getBytes(StandardCharsets.UTF_8)),
                                            s.length(),
                                            -1)
                                    .build());
            List<Record> read = agentSource.read();
            assertEquals(bucket, read.get(0).getHeader("bucket").valueAsString());
            assertEquals("content_changed", read.get(0).getHeader("content_diff").valueAsString());

            state = ((S3Source) agentSource).getStateStorage().get(S3Source.S3SourceState.class);
            assertEquals(10, state.getAllTimeObjects().size());
            assertNotEquals(etag0, state.getAllTimeObjects().get(bucket + "@test-0.txt"));

            // ensure no emit it again
            assertTrue(agentSource.read().isEmpty());

            minioClient.removeObject(
                    RemoveObjectArgs.builder().bucket(bucket).object("test-0.txt").build());
            assertTrue(agentSource.read().isEmpty());
            state = ((S3Source) agentSource).getStateStorage().get(S3Source.S3SourceState.class);
            assertEquals(9, state.getAllTimeObjects().size());
        }
    }
}
