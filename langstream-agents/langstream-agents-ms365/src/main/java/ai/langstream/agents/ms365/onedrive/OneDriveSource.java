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
package ai.langstream.agents.ms365.onedrive;

import static ai.langstream.api.util.ConfigurationUtils.requiredNonEmptyField;

import ai.langstream.agents.ms365.ClientUtil;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderObjectReference;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderSource;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderSourceState;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.util.ConfigurationUtils;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.microsoft.graph.models.*;
import com.microsoft.graph.serviceclient.GraphServiceClient;
import java.util.*;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class OneDriveSource extends StorageProviderSource<OneDriveSource.SharepointSourceState> {

    public static class SharepointSourceState extends StorageProviderSourceState {}

    private GraphServiceClient client;

    private List<String> users;
    private String pathPrefix;

    private Set<String> includeMimeTypes = Set.of();

    private Set<String> excludeMimeTypes = Set.of();

    @Override
    public Class<SharepointSourceState> getStateClass() {
        return SharepointSourceState.class;
    }

    @Override
    public void initializeClientAndConfig(Map<String, Object> configuration) {
        String tenantId =
                requiredNonEmptyField(configuration, "ms-tenant-id", () -> "sharepoint source");
        String clientId =
                requiredNonEmptyField(configuration, "ms-client-id", () -> "sharepoint source");
        String clientSecret =
                requiredNonEmptyField(configuration, "ms-client-secret", () -> "sharepoint source");

        // this is the default scope for the graph api, the actual scopes are bound to the client
        // application
        final String[] scopes = new String[] {"https://graph.microsoft.com/.default"};
        final ClientSecretCredential credential =
                new ClientSecretCredentialBuilder()
                        .clientId(clientId)
                        .tenantId(tenantId)
                        .clientSecret(clientSecret)
                        .build();

        client = new GraphServiceClient(credential, scopes);
        initializeConfig(configuration);
    }

    void initializeConfig(Map<String, Object> configuration) {
        users = ConfigurationUtils.getList("users", configuration);
        if (users.isEmpty()) {
            throw new IllegalArgumentException("At least one user principal must be specified");
        }
        pathPrefix = configuration.getOrDefault("path-prefix", "/").toString();
        if (StringUtils.isNotEmpty(pathPrefix) && !pathPrefix.endsWith("/")) {
            pathPrefix += "/";
        }

        includeMimeTypes = ConfigurationUtils.getSet("include-mime-types", configuration);
        excludeMimeTypes =
                new HashSet<>(ConfigurationUtils.getList("exclude-mime-types", configuration));
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
    public Collection<StorageProviderObjectReference> listObjects() throws Exception {
        List<StorageProviderObjectReference> collect = new ArrayList<>();
        for (String user : users) {
            Drive userDrive = client.users().byUserId(user).drive().get();
            if (userDrive == null) {
                log.warn("No drive found for user {}", user);
                continue;
            }

            Objects.requireNonNull(userDrive.getId());
            final String rootPath = "root:" + pathPrefix;
            log.info(
                    "Listing items for user {} in drive {} at path {}",
                    user,
                    userDrive.getId(),
                    rootPath);
            DriveItem rootItem =
                    client.drives()
                            .byDriveId(userDrive.getId())
                            .items()
                            .byDriveItemId(rootPath)
                            .get();
            if (rootItem == null) {
                log.warn(
                        "No root item found for user drive {} of user {}", userDrive.getId(), user);
                continue;
            }
            int beforeLength = collect.size();
            ClientUtil.collectDriveItems(
                    client,
                    userDrive.getId(),
                    rootItem,
                    includeMimeTypes,
                    excludeMimeTypes,
                    new ClientUtil.DriveItemCollector() {
                        @Override
                        public void collect(String driveId, DriveItem item, String digest) {
                            OneDriveObject oneDriveObject =
                                    new OneDriveObject(item, user, digest, driveId);
                            collect.add(oneDriveObject);
                        }
                    });
            log.info("Found {} items for user {}", collect.size() - beforeLength, user);
        }
        return collect;
    }

    @AllArgsConstructor
    @Data
    private static class OneDriveObject implements StorageProviderObjectReference {
        private final DriveItem item;
        private final String user;
        private final String contentDigest;
        private final String driveId;

        @Override
        public String id() {
            return driveId + "_" + item.getId();
        }

        @Override
        public long size() {
            return item.getSize() == null ? -1 : item.getSize();
        }

        @Override
        public String contentDigest() {
            return contentDigest;
        }

        @Override
        public Collection<Header> additionalRecordHeaders() {
            return List.of(
                    SimpleRecord.SimpleHeader.of("onedrive-item-name", item.getName()),
                    SimpleRecord.SimpleHeader.of("onedrive-item-id", item.getId()),
                    SimpleRecord.SimpleHeader.of("onedrive-user", user),
                    SimpleRecord.SimpleHeader.of("onedrive-drive-id", driveId));
        }
    }

    @Override
    public byte[] downloadObject(StorageProviderObjectReference object) throws Exception {
        OneDriveObject oneDriveObject = (OneDriveObject) object;
        return ClientUtil.downloadDriveItem(
                client, oneDriveObject.getDriveId(), oneDriveObject.getItem());
    }

    @Override
    public void deleteObject(String id) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isStateStorageRequired() {
        return true;
    }
}
