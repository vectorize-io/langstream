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

import ai.langstream.ai.agents.commons.state.LocalDiskStateStorage;
import ai.langstream.ai.agents.commons.state.S3StateStorage;
import ai.langstream.ai.agents.commons.state.StateStorage;
import ai.langstream.ai.agents.commons.state.StateStorageProvider;
import ai.langstream.api.runner.code.*;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.util.ConfigurationUtils;
import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.Item;

import java.io.IOException;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import static ai.langstream.api.util.ConfigurationUtils.getString;

@Slf4j
public class S3Source extends AbstractAgentCode implements AgentSource {
    private Map<String, Object> agentConfiguration;
    private String bucketName;
    private MinioClient minioClient;
    private final Set<String> objectsToCommit = ConcurrentHashMap.newKeySet();
    private int idleTime;

    public static final String ALL_FILES = "*";
    public static final String DEFAULT_EXTENSIONS_FILTER = "pdf,docx,html,htm,md,txt";
    private Set<String> extensions = Set.of();

    private boolean deleteObjects;

    private StateStorage<S3SourceState> stateStorage;

    private TopicProducer deletedObjectsProducer;

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        this.agentConfiguration = configuration;
        bucketName = configuration.getOrDefault("bucketName", "langstream-source").toString();
        String endpoint =
                configuration
                        .getOrDefault("endpoint", "http://minio-endpoint.-not-set:9090")
                        .toString();
        String username = configuration.getOrDefault("access-key", "minioadmin").toString();
        String password = configuration.getOrDefault("secret-key", "minioadmin").toString();
        String region = configuration.getOrDefault("region", "").toString();
        idleTime = Integer.parseInt(configuration.getOrDefault("idle-time", 5).toString());
        extensions =
                Set.of(
                        configuration
                                .getOrDefault("file-extensions", DEFAULT_EXTENSIONS_FILTER)
                                .toString()
                                .split(","));

        deleteObjects = ConfigurationUtils.getBoolean("delete-objects", false, configuration);

        log.info(
                "Connecting to S3 Bucket at {} in region {} with user {}",
                endpoint,
                region,
                username);
        log.info("Getting files with extensions {} (use '*' to no filter)", extensions);

        MinioClient.Builder builder =
                MinioClient.builder().endpoint(endpoint).credentials(username, password);
        if (!region.isBlank()) {
            builder.region(region);
        }
        minioClient = builder.build();

        makeBucketIfNotExists(bucketName);
    }

    private void makeBucketIfNotExists(String bucketName)
            throws Exception {
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
            log.info("Creating bucket {}", bucketName);
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
        } else {
            log.info("Bucket {} already exists", bucketName);
        }
    }

    public record S3SourceState(Map<String, String> allTimeObjects) {

    }

    @Override
    public void setContext(AgentContext context) throws Exception {
        super.setContext(context);
        stateStorage = new StateStorageProvider<S3SourceState>()
                .create(context.getTenant(),
                        agentId(),
                        context.getGlobalAgentId(),
                        agentConfiguration,
                        context.getPersistentStateDirectoryForAgent(agentId()));

        final String deleteObjectsTopic =
                getString("deleted-objects-topic", null, agentConfiguration);
        if (deleteObjectsTopic != null) {
            deleteObjectsTopic =
                    agentContext
                            .getTopicConnectionProvider()
                            .createProducer(
                                    agentContext.getGlobalAgentId(),
                                    deleteObjectsTopic,
                                    Map.of());
            deleteObjectsTopic.start();
        }

    }

    @Override
    public List<Record> read() throws Exception {
        S3SourceState s3SourceState = stateStorage.get(S3SourceState.class);
        if (s3SourceState == null) {
            s3SourceState = new S3SourceState(new HashMap<>());
        }
        Iterable<Result<Item>> results;
        try {
            results = minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName).build());
        } catch (Exception e) {
            log.error("Error listing objects on bucket {}", bucketName, e);
            throw e;
        }

        final Item item = getFirstItem(results);
        if (item == null) {
            log.info("Nothing found, checking deleted objects");
            final Set<String> allNames = new HashSet<>();
            for (Result<Item> result : results) {
                allNames.add(bucketName + "@" + result.get().objectName());
            }
            boolean flush = false;
            for (String allTimeKey : s3SourceState.allTimeObjects().keySet()) {
                if (!allNames.contains(allTimeKey)) {
                    log.info("Object {} was deleted", allTimeKey);
                    s3SourceState.allTimeObjects().remove(allTimeKey);
                    if (deletedObjectsProducer != null) {
                        SimpleRecord simpleRecord = SimpleRecord.of(null, allTimeKey);
                        deletedObjectsProducer.write(simpleRecord).get();
                    }
                    flush = true;
                }
            }
            if (flush) {
                stateStorage.store(s3SourceState);
            }
            log.info("sleeping for {} seconds", idleTime);
            Thread.sleep(idleTime * 1000L);
            return List.of();
        }
        final String name = item.objectName();
        log.info("Found new object {}, size {} KB", name, item.size() / 1024);
        try {
            GetObjectResponse objectResponse =
                    minioClient.getObject(
                            GetObjectArgs.builder()
                                    .bucket(bucketName)
                                    .object(name)
                                    .build());
            objectsToCommit.add(name);
            final byte[] read = objectResponse.readAllBytes();
            final String key = bucketName + "@" + name;
            String oldValue = s3SourceState.allTimeObjects().put(key, item.etag());
            final String contentDiff;
            if (oldValue == null) {
                contentDiff = "new";
            } else if (oldValue.equals(item.etag())) {
                contentDiff = "content_unchanged";
            } else {
                contentDiff = "content_changed";
            }
            processed(0, 1);
            stateStorage.store(s3SourceState);
            return List.of(new S3SourceRecord(read, name, contentDiff));
        } catch (Exception e) {
            log.error("Error reading object {}", name, e);
            throw e;
        }
    }

    private Item getFirstItem(Iterable<Result<Item>> results) throws Exception {
        for (Result<Item> object : results) {
            Item item = object.get();
            String name = item.objectName();
            if (item.isDir()) {
                log.debug("Skipping directory {}", name);
                continue;
            }
            boolean extensionAllowed = isExtensionAllowed(name, extensions);
            if (!extensionAllowed) {
                log.debug("Skipping file with bad extension {}", name);
                continue;
            }
            if (!objectsToCommit.contains(name)) {
                return item;
            } else {
                log.info("Skipping already processed object {}", name);
            }
        }
        return null;
    }

    static boolean isExtensionAllowed(String name, Set<String> extensions) {
        if (extensions.contains(ALL_FILES)) {
            return true;
        }
        String extension;
        int extensionIndex = name.lastIndexOf('.');
        if (extensionIndex < 0 || extensionIndex == name.length() - 1) {
            extension = "";
        } else {
            extension = name.substring(extensionIndex + 1);
        }
        return extensions.contains(extension);
    }

    @Override
    protected Map<String, Object> buildAdditionalInfo() {
        return Map.of("bucketName", bucketName);
    }

    @Override
    public void commit(List<Record> records) throws Exception {
        if (!deleteObjects) {
            return;
        }
        for (Record record : records) {
            S3SourceRecord s3SourceRecord = (S3SourceRecord) record;
            String objectName = s3SourceRecord.name;
            log.info("Removing object {}", objectName);
            minioClient.removeObject(
                    RemoveObjectArgs.builder().bucket(bucketName).object(objectName).build());
            objectsToCommit.remove(objectName);
        }
    }

    private static final class S3SourceRecord implements Record {
        private final byte[] read;
        private final String name;
        private final String contentDiff;

        public S3SourceRecord(byte[] read, String name, String contentDiff) {
            this.read = read;
            this.name = name;
            this.contentDiff = contentDiff;
        }

        /**
         * the key is used for routing, so it is better to set it to something meaningful. In case
         * of retransmission the message will be sent to the same partition.
         *
         * @return the key
         */
        @Override
        public Object key() {
            return name;
        }

        @Override
        public Object value() {
            return read;
        }

        @Override
        public String origin() {
            return null;
        }

        @Override
        public Long timestamp() {
            return System.currentTimeMillis();
        }

        @Override
        public Collection<Header> headers() {
            return List.of(
                    new SimpleRecord.SimpleHeader("name", name),
                    new SimpleRecord.SimpleHeader("content_diff", contentDiff));
        }

    }
}
