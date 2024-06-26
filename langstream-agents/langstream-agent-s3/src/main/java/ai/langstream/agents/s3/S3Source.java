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

import static ai.langstream.api.util.ConfigurationUtils.*;

import ai.langstream.ai.agents.commons.storage.provider.StorageProviderObjectReference;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderSource;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderSourceState;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.util.ConfigurationUtils;
import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.messages.Item;
import java.util.*;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class S3Source extends StorageProviderSource<S3Source.S3SourceState> {

    public static class S3SourceState extends StorageProviderSourceState {}

    private String bucketName;
    private MinioClient minioClient;
    private int idleTime;
    private String deletedObjectsTopic;
    private String sourceActivitySummaryTopic;

    private List<String> sourceActivitySummaryEvents;

    private int sourceActivitySummaryNumEventsThreshold;
    private int sourceActivitySummaryTimeSecondsThreshold;

    public static final String ALL_FILES = "*";
    public static final String DEFAULT_EXTENSIONS_FILTER = "pdf,docx,html,htm,md,txt";
    private Set<String> extensions = Set.of();

    private boolean deleteObjects;
    private Collection<Header> sourceRecordHeaders;

    @Override
    public Class<S3SourceState> getStateClass() {
        return S3SourceState.class;
    }

    @Override
    @SneakyThrows
    public void initializeClientAndBucket(Map<String, Object> configuration) {
        bucketName = configuration.getOrDefault("bucketName", "langstream-source").toString();
        String endpoint =
                configuration
                        .getOrDefault("endpoint", "http://minio-endpoint.-not-set:9090")
                        .toString();
        String username = configuration.getOrDefault("access-key", "minioadmin").toString();
        String password = configuration.getOrDefault("secret-key", "minioadmin").toString();
        String region = configuration.getOrDefault("region", "").toString();
        idleTime = Integer.parseInt(configuration.getOrDefault("idle-time", 5).toString());
        deletedObjectsTopic = getString("deleted-objects-topic", null, configuration);
        sourceActivitySummaryTopic =
                getString("source-activity-summary-topic", null, configuration);
        sourceActivitySummaryEvents = getList("source-activity-summary-events", configuration);
        sourceActivitySummaryNumEventsThreshold =
                getInt("source-activity-summary-events-threshold", 0, configuration);
        sourceActivitySummaryTimeSecondsThreshold =
                getInt("source-activity-summary-time-seconds-threshold", 30, configuration);
        if (sourceActivitySummaryTimeSecondsThreshold < 0) {
            throw new IllegalArgumentException(
                    "source-activity-summary-time-seconds-threshold must be > 0");
        }

        extensions =
                Set.of(
                        configuration
                                .getOrDefault("file-extensions", DEFAULT_EXTENSIONS_FILTER)
                                .toString()
                                .split(","));

        deleteObjects = ConfigurationUtils.getBoolean("delete-objects", true, configuration);

        sourceRecordHeaders =
                getMap("source-record-headers", Map.of(), configuration).entrySet().stream()
                        .map(
                                entry ->
                                        SimpleRecord.SimpleHeader.of(
                                                entry.getKey(), entry.getValue()))
                        .collect(Collectors.toUnmodifiableList());

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
        S3Utils.makeBucketIfNotExists(minioClient, bucketName);
    }

    @Override
    public String getBucketName() {
        return bucketName;
    }

    @Override
    public boolean isDeleteObjects() {
        return deleteObjects;
    }

    @Override
    public int getIdleTime() {
        return idleTime;
    }

    @Override
    public String getDeletedObjectsTopic() {
        return deletedObjectsTopic;
    }

    @Override
    public String getSourceActivitySummaryTopic() {
        return sourceActivitySummaryTopic;
    }

    @Override
    public List<String> getSourceActivitySummaryEvents() {
        return sourceActivitySummaryEvents;
    }

    @Override
    public int getSourceActivitySummaryNumEventsThreshold() {
        return sourceActivitySummaryNumEventsThreshold;
    }

    @Override
    public int getSourceActivitySummaryTimeSecondsThreshold() {
        return sourceActivitySummaryTimeSecondsThreshold;
    }

    @Override
    public List<StorageProviderObjectReference> listObjects() throws Exception {
        Iterable<Result<Item>> results;
        try {
            results = minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName).build());
        } catch (Exception e) {
            log.error("Error listing objects on bucket {}", bucketName, e);
            throw e;
        }

        List<StorageProviderObjectReference> refs = new ArrayList<>();
        for (Result<Item> result : results) {
            Item item = result.get();
            final String name = item.objectName();
            if (item.isDir()) {
                log.debug("Skipping directory {}", name);
                continue;
            }
            boolean extensionAllowed = isExtensionAllowed(name, extensions);
            if (!extensionAllowed) {
                log.debug("Skipping file with bad extension {}", name);
                continue;
            }

            StorageProviderObjectReference ref =
                    new StorageProviderObjectReference() {
                        @Override
                        public String name() {
                            return item.objectName();
                        }

                        @Override
                        public long size() {
                            return item.size();
                        }

                        @Override
                        public String contentDigest() {
                            return item.etag();
                        }
                    };
            refs.add(ref);
        }
        return refs;
    }

    @Override
    public byte[] downloadObject(String name) throws Exception {
        GetObjectResponse objectResponse =
                minioClient.getObject(
                        GetObjectArgs.builder().bucket(bucketName).object(name).build());
        return objectResponse.readAllBytes();
    }

    @Override
    public void deleteObject(String name) throws Exception {
        minioClient.removeObject(
                RemoveObjectArgs.builder().bucket(bucketName).object(name).build());
    }

    @Override
    public Collection<Header> getSourceRecordHeaders() {
        return sourceRecordHeaders;
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
    public void close() throws Exception {
        super.close();
        if (minioClient != null) {
            minioClient.close();
        }
    }
}
