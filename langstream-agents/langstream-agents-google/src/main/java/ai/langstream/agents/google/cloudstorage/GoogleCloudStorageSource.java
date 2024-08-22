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

    private String pathPrefix;
    private boolean recursive;
    private boolean deleteObjects;
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
    public void initializeClientAndConfig(Map<String, Object> configuration) {
        bucketName = getString("bucket-name", "langstream-gcs-source", configuration);
        initClientWithAutoRefreshToken(
                requiredNonEmptyField(
                        configuration,
                        "service-account-json",
                        () -> "google cloud storage service"),
                bucketName);
        deleteObjects = ConfigurationUtils.getBoolean("delete-objects", true, configuration);
        pathPrefix = configuration.getOrDefault("path-prefix", "").toString();
        if (StringUtils.isNotEmpty(pathPrefix) && !pathPrefix.endsWith("/")) {
            pathPrefix += "/";
        }
        recursive = getBoolean("recursive", false, configuration);
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
                        public String id() {
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
    public byte[] downloadObject(StorageProviderObjectReference object) throws Exception {
        return gcsClient.readAllBytes(bucketName, object.id());
    }

    @Override
    public void deleteObject(String id) throws Exception {
        gcsClient.delete(bucketName, id);
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
    public void close() {
        super.close();
        if (credentials != null) {
            credentials.close();
        }
        if (gcsClient != null) {
            try {
                gcsClient.close();
            } catch (Exception e) {
                log.error("Error closing GCS client", e);
            }
        }
    }
}
