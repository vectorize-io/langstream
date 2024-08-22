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

import static ai.langstream.api.util.ConfigurationUtils.*;
import static ai.langstream.api.util.ConfigurationUtils.getInt;

import ai.langstream.ai.agents.commons.storage.provider.StorageProviderObjectReference;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderSource;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderSourceState;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.util.ConfigurationUtils;
import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class AzureBlobStorageSource
        extends StorageProviderSource<AzureBlobStorageSource.AzureBlobStorageSourceState> {

    public static class AzureBlobStorageSourceState extends StorageProviderSourceState {}

    private BlobContainerClient client;
    private String pathPrefix;
    private boolean recursive;

    private boolean deleteObjects;

    public static final String ALL_FILES = "*";
    public static final String DEFAULT_EXTENSIONS_FILTER = "pdf,docx,html,htm,md,txt";
    private Set<String> extensions = Set.of();

    public static BlobContainerClient createContainerClient(Map<String, Object> configuration) {
        return createContainerClient(
                ConfigurationUtils.getString("container", "langstream-azure-source", configuration),
                ConfigurationUtils.requiredNonEmptyField(
                        configuration, "endpoint", () -> "azure blob storage source"),
                ConfigurationUtils.getString("sas-token", null, configuration),
                ConfigurationUtils.getString("storage-account-name", null, configuration),
                ConfigurationUtils.getString("storage-account-key", null, configuration),
                ConfigurationUtils.getString(
                        "storage-account-connection-string", null, configuration));
    }

    static BlobContainerClient createContainerClient(
            String container,
            String endpoint,
            String sasToken,
            String storageAccountName,
            String storageAccountKey,
            String storageAccountConnectionString) {

        BlobContainerClientBuilder containerClientBuilder = new BlobContainerClientBuilder();
        if (sasToken != null) {
            containerClientBuilder.sasToken(sasToken);
            log.info("Connecting to Azure at {} with SAS token", endpoint);
        } else if (storageAccountName != null) {
            containerClientBuilder.credential(
                    new StorageSharedKeyCredential(storageAccountName, storageAccountKey));
            log.info(
                    "Connecting to Azure at {} with account name {}", endpoint, storageAccountName);
        } else if (storageAccountConnectionString != null) {
            log.info("Connecting to Azure at {} with connection string", endpoint);
            containerClientBuilder.credential(
                    StorageSharedKeyCredential.fromConnectionString(
                            storageAccountConnectionString));
        } else {
            throw new IllegalArgumentException(
                    "Either sas-token, account-name/account-key or account-connection-string must be provided");
        }

        containerClientBuilder.endpoint(endpoint);
        containerClientBuilder.containerName(container);

        final BlobContainerClient containerClient = containerClientBuilder.buildClient();
        log.info(
                "Connected to Azure to account {}, container {}",
                containerClient.getAccountName(),
                containerClient.getBlobContainerName());

        if (!containerClient.exists()) {
            log.info("Creating container");
            containerClient.createIfNotExists();
            log.info("Created container {}", containerClient.getBlobContainerName());
        } else {
            log.info("Container already exists");
        }
        return containerClient;
    }

    @Override
    public Class<AzureBlobStorageSourceState> getStateClass() {
        return AzureBlobStorageSourceState.class;
    }

    @Override
    public void initializeClientAndConfig(Map<String, Object> configuration) {
        client = createContainerClient(configuration);
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
        return client.getBlobContainerName();
    }

    @Override
    public boolean isDeleteObjects() {
        return deleteObjects;
    }

    @Override
    public List<StorageProviderObjectReference> listObjects() throws Exception {
        final PagedIterable<BlobItem> blobs;
        try {
            ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
            listBlobsOptions.setPrefix(pathPrefix);
            blobs = client.listBlobs(listBlobsOptions, null);
        } catch (Exception e) {
            log.error("Error listing blobs on container {}", client.getBlobContainerName(), e);
            throw e;
        }
        List<StorageProviderObjectReference> refs = new ArrayList<>();
        for (BlobItem blob : blobs) {
            final String name = blob.getName();
            if (blob.isDeleted()) {
                log.debug("Skipping blob {}. deleted status", name);
                continue;
            }
            boolean extensionAllowed = isExtensionAllowed(name, extensions);
            if (!extensionAllowed) {
                log.debug("Skipping blob with bad extension {}", name);
                continue;
            }
            if (!recursive) {
                final String withoutPrefix = name.substring(pathPrefix.length());
                int lastSlash = withoutPrefix.lastIndexOf('/');
                if (lastSlash >= 0) {
                    log.debug("Skipping blob {}. recursive is disabled", name);
                    continue;
                }
            }
            final String eTag = blob.getProperties().getETag();
            final long size =
                    blob.getProperties().getContentLength() == null
                            ? -1
                            : blob.getProperties().getContentLength();
            StorageProviderObjectReference ref =
                    new StorageProviderObjectReference() {
                        @Override
                        public String id() {
                            return blob.getName();
                        }

                        @Override
                        public long size() {
                            return size;
                        }

                        @Override
                        public String contentDigest() {
                            return eTag;
                        }
                    };
            refs.add(ref);
        }
        return refs;
    }

    @Override
    public byte[] downloadObject(StorageProviderObjectReference object) throws Exception {
        return client.getBlobClient(object.id()).downloadContent().toBytes();
    }

    @Override
    public void deleteObject(String id) throws Exception {
        client.getBlobClient(id).deleteIfExists();
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
    protected Map<String, Object> buildAdditionalInfo() {
        Map<String, Object> parentInfo = new HashMap<>(super.buildAdditionalInfo());
        parentInfo.put("container", client.getBlobContainerName());
        return parentInfo;
    }
}
