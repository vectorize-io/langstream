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
package ai.langstream.agents.google.cloudstorage;

import static ai.langstream.api.util.ConfigurationUtils.*;

import ai.langstream.agents.google.utils.AutoRefreshGoogleCredentials;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderObjectReference;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderSource;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderSourceState;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.util.ConfigurationUtils;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class GoogleCloudStorageSource
        extends StorageProviderSource<GoogleCloudStorageSource.GCSSourceState> {

    public static class GCSSourceState extends StorageProviderSourceState {}

    private String bucketName;
    private Storage gcsClient;
    private AutoRefreshGoogleCredentials credentials;

    private int idleTime;

    private String deletedObjectsTopic;
    private String pathPrefix;
    private boolean recursive;
    private boolean deleteObjects;
    private Collection<Header> sourceRecordHeaders;
    private String sourceActivitySummaryTopic;

    private List<String> sourceActivitySummaryEvents;

    private int sourceActivitySummaryNumEventsThreshold;
    private int sourceActivitySummaryTimeSecondsThreshold;

    public static final String ALL_FILES = "*";
    public static final String DEFAULT_EXTENSIONS_FILTER = "pdf,docx,html,htm,md,txt";
    private Set<String> extensions = Set.of();

    @Override
    public Class<GCSSourceState> getStateClass() {
        return GCSSourceState.class;
    }

    @SneakyThrows
    private void initClientWithAutoRefreshToken(String serviceAccountJson, String bucketName) {

        StorageOptions storageOptions = initStorageOptions(serviceAccountJson);
        gcsClient = storageOptions.getService();

        Bucket bucket = gcsClient.get(bucketName);
        if (bucket == null) {
            log.info("Bucket {} does not exist, creating it", bucketName);
            gcsClient.create(BucketInfo.newBuilder(bucketName).build());
        }
    }

    StorageOptions initStorageOptions(String serviceAccountJson) throws IOException {
        GoogleCredentials googleCredentials =
                GoogleCredentials.fromStream(
                                new ByteArrayInputStream(
                                        serviceAccountJson.getBytes(StandardCharsets.UTF_8)))
                        .createScoped("https://www.googleapis.com/auth/devstorage.read_write");

        credentials = new AutoRefreshGoogleCredentials(googleCredentials);
        return StorageOptions.newBuilder().setCredentials(googleCredentials).build();
    }

    @Override
    public void initializeClientAndBucket(Map<String, Object> configuration) {
        bucketName = getString("bucket-name", "langstream-gcs-source", configuration);
        initClientWithAutoRefreshToken(
                requiredNonEmptyField(
                        configuration,
                        "service-account-json",
                        () -> "google cloud storage service"),
                bucketName);
        idleTime = Integer.parseInt(configuration.getOrDefault("idle-time", 5).toString());
        deletedObjectsTopic = getString("deleted-objects-topic", null, configuration);
        deleteObjects = ConfigurationUtils.getBoolean("delete-objects", true, configuration);
        sourceRecordHeaders =
                getMap("source-record-headers", Map.of(), configuration).entrySet().stream()
                        .map(
                                entry ->
                                        SimpleRecord.SimpleHeader.of(
                                                entry.getKey(), entry.getValue()))
                        .collect(Collectors.toUnmodifiableList());
        pathPrefix = configuration.getOrDefault("path-prefix", "").toString();
        if (StringUtils.isNotEmpty(pathPrefix) && !pathPrefix.endsWith("/")) {
            pathPrefix += "/";
        }
        recursive = getBoolean("recursive", false, configuration);
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

        log.info("Getting files with extensions {} (use '*' to no filter)", extensions);
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
        Page<Blob> blobs = gcsClient.list(bucketName, Storage.BlobListOption.prefix(pathPrefix));

        List<StorageProviderObjectReference> all = new ArrayList<>();
        for (Blob blob : blobs.iterateAll()) {
            if (blob.isDirectory()) {
                log.debug("Skipping blob {}. is a directory", blob.getName());
                continue;
            }
            boolean extensionAllowed = isExtensionAllowed(blob.getName(), extensions);
            if (!extensionAllowed) {
                log.debug("Skipping blob with bad extension {}", blob.getName());
                continue;
            }
            if (!recursive) {
                final String withoutPrefix = blob.getName().substring(pathPrefix.length());
                int lastSlash = withoutPrefix.lastIndexOf('/');
                if (lastSlash >= 0) {
                    log.debug("Skipping blob {}. recursive is disabled", blob.getName());
                    continue;
                }
            }

            all.add(
                    new StorageProviderObjectReference() {
                        @Override
                        public String name() {
                            return blob.getName();
                        }

                        @Override
                        public long size() {
                            return blob.getSize() == null ? -1 : blob.getSize();
                        }

                        @Override
                        public String contentDigest() {
                            return blob.getEtag();
                        }
                    });
        }
        return all;
    }

    @Override
    public byte[] downloadObject(String name) throws Exception {
        return gcsClient.readAllBytes(bucketName, name);
    }

    @Override
    public void deleteObject(String name) throws Exception {
        gcsClient.delete(bucketName, name);
    }

    @Override
    public Collection<Header> getSourceRecordHeaders() {
        return sourceRecordHeaders;
    }

    @Override
    public boolean isStateStorageRequired() {
        return false;
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
        if (credentials != null) {
            credentials.close();
        }
        if (gcsClient != null) {
            gcsClient.close();
        }
    }
}
