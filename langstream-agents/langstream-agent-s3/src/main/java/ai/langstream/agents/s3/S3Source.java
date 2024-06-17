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

import static ai.langstream.api.util.ConfigurationUtils.getString;

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
import io.minio.messages.Item;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

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

    @Getter private StateStorage<S3SourceState> stateStorage;

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

        deleteObjects = ConfigurationUtils.getBoolean("delete-objects", true, configuration);

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

    private void makeBucketIfNotExists(String bucketName) throws Exception {
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
            log.info("Creating bucket {}", bucketName);
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
        } else {
            log.info("Bucket {} already exists", bucketName);
        }
    }

    public record S3SourceState(Map<String, String> allTimeObjects) {}

    @Override
    public void setContext(AgentContext context) throws Exception {
        super.setContext(context);
        stateStorage =
                new StateStorageProvider<S3SourceState>()
                        .create(
                                context.getTenant(),
                                agentId(),
                                context.getGlobalAgentId(),
                                agentConfiguration,
                                context.getPersistentStateDirectoryForAgent(agentId()));

        final String deleteObjectsTopic =
                getString("deleted-objects-topic", null, agentConfiguration);
        if (deleteObjectsTopic != null) {
            deletedObjectsProducer =
                    agentContext
                            .getTopicConnectionProvider()
                            .createProducer(
                                    agentContext.getGlobalAgentId(), deleteObjectsTopic, Map.of());
            deletedObjectsProducer.start();
        }
    }

    @Override
    public List<Record> read() throws Exception {

        Iterable<Result<Item>> results;
        try {
            results = minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName).build());
        } catch (Exception e) {
            log.error("Error listing objects on bucket {}", bucketName, e);
            throw e;
        }

        final Item item = getFirstItem(results);
        if (item == null) {
            log.info("No objects to emit");
            if (stateStorage != null) {
                log.info("Checking for deleted objects");
                S3SourceState s3SourceState = stateStorage.get(S3SourceState.class);
                if (s3SourceState == null) {
                    s3SourceState = new S3SourceState(new HashMap<>());
                }

                final Set<String> allNames = new HashSet<>();
                for (Result<Item> result : results) {
                    allNames.add(formatAllTimeObjectsKey(result.get().objectName()));
                }
                Set<String> toRemove = new HashSet<>();
                for (String allTimeKey : s3SourceState.allTimeObjects().keySet()) {
                    if (!allNames.contains(allTimeKey)) {
                        log.info("Object {} was deleted", allTimeKey);
                        toRemove.add(allTimeKey);
                    }
                }
                for (String allTimeKey : toRemove) {
                    s3SourceState.allTimeObjects().remove(allTimeKey);
                    if (deletedObjectsProducer != null) {
                        SimpleRecord simpleRecord =
                                SimpleRecord.of(
                                        bucketName, allTimeKey.replace(bucketName + "@", ""));
                        deletedObjectsProducer.write(simpleRecord).get();
                    }
                }
                if (!toRemove.isEmpty()) {
                    stateStorage.store(s3SourceState);
                }
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
                            GetObjectArgs.builder().bucket(bucketName).object(name).build());
            if (deleteObjects) {
                objectsToCommit.add(name);
            }
            final byte[] read = objectResponse.readAllBytes();

            final String contentDiff;
            if (stateStorage != null) {
                S3SourceState s3SourceState = stateStorage.get(S3SourceState.class);
                if (s3SourceState == null) {
                    s3SourceState = new S3SourceState(new HashMap<>());
                }
                final String key = formatAllTimeObjectsKey(name);
                String oldValue = s3SourceState.allTimeObjects().put(key, item.etag());
                contentDiff = oldValue == null ? "new" : "content_changed";
                stateStorage.store(s3SourceState);
            } else {
                contentDiff = "no_state_storage";
            }
            processed(0, 1);
            return List.of(new S3SourceRecord(read, bucketName, name, contentDiff));
        } catch (Exception e) {
            log.error("Error reading object {}", name, e);
            throw e;
        }
    }

    private Item getFirstItem(Iterable<Result<Item>> results) throws Exception {
        S3SourceState s3SourceState = null;
        if (stateStorage != null) {
            log.info("Checking for deleted objects");
            s3SourceState = stateStorage.get(S3SourceState.class);
            if (s3SourceState == null) {
                s3SourceState = new S3SourceState(new HashMap<>());
            }
        }
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
            if (!objectsToCommit.contains(name) || !deleteObjects) {
                if (s3SourceState != null) {
                    String allTimeETag =
                            s3SourceState.allTimeObjects().get(formatAllTimeObjectsKey(name));
                    if (allTimeETag != null && allTimeETag.equals(item.etag())) {
                        log.info("Object {} didn't change since last time, skipping", name);
                        continue;
                    }
                }
                return item;
            } else {
                log.info("Skipping already processed object {}", name);
            }
        }
        return null;
    }

    @NotNull
    private String formatAllTimeObjectsKey(String name) {
        return bucketName + "@" + name;
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
        private final String bucket;
        private final String name;
        private final String contentDiff;

        public S3SourceRecord(byte[] read, String bucket, String name, String contentDiff) {
            this.read = read;
            this.bucket = bucket;
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
                    new SimpleRecord.SimpleHeader("bucket", bucket),
                    new SimpleRecord.SimpleHeader("content_diff", contentDiff));
        }
    }
}
