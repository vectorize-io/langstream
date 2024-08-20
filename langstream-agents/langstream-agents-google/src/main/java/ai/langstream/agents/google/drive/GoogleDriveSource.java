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
package ai.langstream.agents.google.drive;

import static ai.langstream.api.util.ConfigurationUtils.*;

import ai.langstream.agents.google.utils.AutoRefreshGoogleCredentials;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderObjectReference;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderSource;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderSourceState;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.util.ConfigurationUtils;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GoogleDriveSource extends StorageProviderSource<GoogleDriveSource.GDriveSourceState> {

    public static class GDriveSourceState extends StorageProviderSourceState {}

    private GDriveClient client;
    private List<String> rootParents;

    private int idleTime;

    private String deletedObjectsTopic;
    private Collection<Header> sourceRecordHeaders;
    private String sourceActivitySummaryTopic;

    private List<String> sourceActivitySummaryEvents;

    private int sourceActivitySummaryNumEventsThreshold;
    private int sourceActivitySummaryTimeSecondsThreshold;

    private Set<String> includeMimeTypes = Set.of();

    private Set<String> excludeMimeTypes = Set.of();

    @Override
    public Class<GDriveSourceState> getStateClass() {
        return GDriveSourceState.class;
    }

    public static class GDriveClient implements AutoCloseable {
        private final AutoRefreshGoogleCredentials credentials;
        @Getter private final Drive client;
        private final NetHttpTransport transport;

        public GDriveClient(String credentialsJson) {
            this(credentialsJson, DriveScopes.DRIVE_READONLY);
        }

        @SneakyThrows
        public GDriveClient(String credentialsJson, String scope) {
            transport = GoogleNetHttpTransport.newTrustedTransport();

            GoogleCredentials googleCredentials =
                    GoogleCredentials.fromStream(
                                    new ByteArrayInputStream(
                                            credentialsJson.getBytes(StandardCharsets.UTF_8)))
                            .createScoped(scope);

            credentials = new AutoRefreshGoogleCredentials(googleCredentials);

            final GsonFactory jsonFactory = GsonFactory.getDefaultInstance();
            client =
                    new Drive.Builder(
                                    transport,
                                    jsonFactory,
                                    new HttpCredentialsAdapter(googleCredentials))
                            .setApplicationName("langstream")
                            .build();
        }

        @Override
        public void close() throws Exception {
            if (transport != null) {
                transport.shutdown();
            }
            if (credentials != null) {
                credentials.close();
            }
        }
    }

    @SneakyThrows
    void initClientWithAutoRefreshToken(String serviceAccountJson) {
        client = new GDriveClient(serviceAccountJson);
    }

    @Override
    public void initializeClientAndBucket(Map<String, Object> configuration) {
        initClientWithAutoRefreshToken(
                requiredNonEmptyField(
                        configuration,
                        "service-account-json",
                        () -> "google drive source service"));
        initializeConfig(configuration);
    }

    void initializeConfig(Map<String, Object> configuration) {
        rootParents = ConfigurationUtils.getList("root-parents", configuration);
        idleTime = Integer.parseInt(configuration.getOrDefault("idle-time", 5).toString());
        deletedObjectsTopic = getString("deleted-objects-topic", null, configuration);
        sourceRecordHeaders =
                getMap("source-record-headers", Map.of(), configuration).entrySet().stream()
                        .map(
                                entry ->
                                        SimpleRecord.SimpleHeader.of(
                                                entry.getKey(), entry.getValue()))
                        .collect(Collectors.toUnmodifiableList());
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
        includeMimeTypes = ConfigurationUtils.getSet("include-mime-types", configuration);
        excludeMimeTypes =
                new HashSet<>(ConfigurationUtils.getList("exclude-mime-types", configuration));
        excludeMimeTypes.add("application/vnd.google-apps.folder");
        if (includeMimeTypes.isEmpty()) {
            log.info("Filtering out files with mime types: {}", excludeMimeTypes);
        } else {
            log.info("Filtering only files with mime types: {}", includeMimeTypes);
        }
    }

    @Override
    public String getBucketName() {
        return "";
    }

    @Override
    public boolean isDeleteObjects() {
        return false;
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
    public Collection<StorageProviderObjectReference> listObjects() throws Exception {
        Map<String, List<String>> tree = new HashMap<>();
        Map<String, StorageProviderObjectReference> collect = new LinkedHashMap<>();

        String pageToken = null;
        do {
            FileList result =
                    client.getClient()
                            .files()
                            .list()
                            .setPageSize(10)
                            .setPageToken(pageToken)
                            .setFields(
                                    "nextPageToken, files(id, name, parents, size, sha256Checksum, mimeType, modifiedTime, version)")
                            .setOrderBy("modifiedTime")
                            .setQ("trashed=false")
                            .execute();
            List<File> files = result.getFiles();
            inspectFiles(files, collect, tree);

            pageToken = result.getNextPageToken();
        } while (pageToken != null);
        filterRootParents(collect, tree);
        log.info("Found {} files", collect.size());

        return collect.values();
    }

    void filterRootParents(
            Map<String, StorageProviderObjectReference> collect, Map<String, List<String>> tree) {
        if (rootParents != null && !rootParents.isEmpty()) {
            collect.entrySet().removeIf(e -> !isParentAllowed(e.getKey(), tree, rootParents));
        }
    }

    private static boolean isParentAllowed(
            String key, Map<String, List<String>> tree, List<String> allowedParents) {
        if (allowedParents.contains(key)) {
            return true;
        }
        List<String> parents = tree.get(key);
        if (parents == null || parents.isEmpty()) {
            return false;
        }
        for (String parent : parents) {
            if (allowedParents.contains(parent)) {
                return true;
            }
            if (isParentAllowed(parent, tree, allowedParents)) {
                return true;
            }
        }
        return false;
    }

    void inspectFiles(
            List<File> files,
            Map<String, StorageProviderObjectReference> collect,
            Map<String, List<String>> parentsTree) {
        for (File file : files) {
            if (rootParents != null && !rootParents.isEmpty()) {
                parentsTree.put(file.getId(), file.getParents());
            }
            if (log.isDebugEnabled()) {
                log.debug(
                        "File {} ({}), mime type {}, size {}, sha {}, parents {}, last modified time {}",
                        file.getName(),
                        file.getId(),
                        file.getMimeType(),
                        file.getSize(),
                        file.getSha256Checksum(),
                        file.getParents(),
                        file.getModifiedTime() == null
                                ? "NULL"
                                : file.getModifiedTime().toString());
            }

            if (!includeMimeTypes.isEmpty() && !includeMimeTypes.contains(file.getMimeType())) {
                log.debug(
                        "Skipping file {} ({}) due to mime type {}",
                        file.getName(),
                        file.getId(),
                        file.getMimeType());
                continue;
            }
            if (!excludeMimeTypes.isEmpty() && excludeMimeTypes.contains(file.getMimeType())) {
                log.debug(
                        "Skipping file {} ({}) due to excluded mime type {}",
                        file.getName(),
                        file.getId(),
                        file.getMimeType());
                continue;
            }

            final String digest;
            if (file.getSha256Checksum() != null) {
                digest = file.getSha256Checksum();
            } else if (file.getModifiedTime() != null) {
                digest = file.getModifiedTime().getValue() + "";
            } else if (file.getVersion() != null) {
                log.warn(
                        "Using file version as digest for {}, this might be end up in duplicated processing",
                        file.getId());
                digest = file.getVersion() + "";
            } else {
                log.error("Not able to compute a digest for {}, skipping", file.getId());
                continue;
            }
            collect.put(
                    file.getId(),
                    new StorageProviderObjectReference() {
                        @Override
                        public String name() {
                            return file.getId();
                        }

                        @Override
                        public long size() {
                            return file.getSize() == null ? -1 : file.getSize();
                        }

                        @Override
                        public String contentDigest() {
                            return digest;
                        }

                        @Override
                        public Collection<Header> additionalRecordHeaders() {
                            return List.of(
                                    SimpleRecord.SimpleHeader.of("drive-filename", file.getName()));
                        }
                    });
        }
    }

    @Override
    public byte[] downloadObject(String name) throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            client.getClient().files().get(name).executeMediaAndDownloadTo(outputStream);

        } catch (HttpResponseException responseException) {
            if (responseException.getContent().contains("fileNotDownloadable")) {
                // force pdf since most of the google formats are compatible
                // https://developers.google.com/drive/api/guides/ref-export-formats
                // in the future we could add a mapping in the source configuration
                client.getClient()
                        .files()
                        .export(name, "application/pdf")
                        .executeMediaAndDownloadTo(outputStream);
            } else {
                throw responseException;
            }
        }
        return outputStream.toByteArray();
    }

    @Override
    public void deleteObject(String name) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Header> getSourceRecordHeaders() {
        return sourceRecordHeaders;
    }

    @Override
    public boolean isStateStorageRequired() {
        return true;
    }

    @Override
    public void close() {
        super.close();
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                log.error("Error closing client", e);
            }
        }
    }
}
