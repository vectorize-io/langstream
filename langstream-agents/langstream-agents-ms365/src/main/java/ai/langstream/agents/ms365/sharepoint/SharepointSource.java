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
package ai.langstream.agents.ms365.sharepoint;

import static ai.langstream.api.util.ConfigurationUtils.*;

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
import com.microsoft.graph.sites.getallsites.GetAllSitesGetResponse;
import java.util.*;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SharepointSource
        extends StorageProviderSource<SharepointSource.SharepointSourceState> {

    public static class SharepointSourceState extends StorageProviderSourceState {}

    private GraphServiceClient client;

    private List<String> includeOnlySites;

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
        includeOnlySites = ConfigurationUtils.getList("sites", configuration);

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

        GetAllSitesGetResponse getAllSitesGetResponse = client.sites().getAllSites().get();
        if (getAllSitesGetResponse == null) {
            throw new IllegalStateException("No sites found, maybe not enabled or no permissions?");
        }
        List<Site> sites = getAllSitesGetResponse.getValue();
        if (sites == null) {
            log.info("No sites found");
            return Collections.emptyList();
        }
        log.info("Found {} sites", sites.size());
        if (!includeOnlySites.isEmpty()) {
            log.info("Filtering sites to include only {}", includeOnlySites);
            sites = new ArrayList<>(sites);
            sites.removeIf(
                    site -> {
                        if (includeOnlySites.contains(site.getId())) {
                            return false;
                        }
                        if (includeOnlySites.contains(site.getDisplayName())) {
                            return false;
                        }
                        log.info("Excluding site {} ({})", site.getDisplayName(), site.getId());
                        return true;
                    });
        }

        List<StorageProviderObjectReference> collect = new ArrayList<>();
        for (Site site : sites) {
            log.info("Listing site {} ({})", site.getDisplayName(), site.getId());
            Objects.requireNonNull(site.getId());
            DriveCollectionResponse driveCollectionResponse =
                    client.sites().bySiteId(site.getId()).drives().get();
            if (driveCollectionResponse == null) {
                throw new IllegalStateException(
                        "No drives found, maybe not enabled or no permissions?");
            }
            List<Drive> drives = driveCollectionResponse.getValue();
            if (drives == null) {
                continue;
            }
            int beforeLength = collect.size();
            for (Drive drive : drives) {
                Objects.requireNonNull(drive.getId());
                DriveItem rootItem = client.drives().byDriveId(drive.getId()).root().get();
                if (rootItem == null) {
                    log.warn("No root item found for drive {}", drive.getId());
                    continue;
                }
                ClientUtil.collectDriveItems(
                        client,
                        drive.getId(),
                        rootItem,
                        includeMimeTypes,
                        excludeMimeTypes,
                        new ClientUtil.DriveItemCollector() {
                            @Override
                            public void collect(String driveId, DriveItem item, String digest) {
                                SharepointObject sharepointObject =
                                        new SharepointObject(item, site.getId(), digest, driveId);
                                collect.add(sharepointObject);
                            }
                        });
            }
            log.info(
                    "Found {} items in site {} ({})",
                    collect.size() - beforeLength,
                    site.getDisplayName(),
                    site.getId());
        }
        return collect;
    }

    @AllArgsConstructor
    @Data
    private static class SharepointObject implements StorageProviderObjectReference {
        private final DriveItem item;
        private final String siteId;
        private final String contentDigest;
        private final String driveId;

        @Override
        public String id() {
            return siteId + "_" + item.getId();
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
                    SimpleRecord.SimpleHeader.of("sharepoint-item-name", item.getName()),
                    SimpleRecord.SimpleHeader.of("sharepoint-item-id", item.getId()),
                    SimpleRecord.SimpleHeader.of("sharepoint-drive-id", driveId),
                    SimpleRecord.SimpleHeader.of("sharepoint-site-id", siteId));
        }
    }

    @Override
    public byte[] downloadObject(StorageProviderObjectReference object) throws Exception {
        SharepointObject sharepointObject = (SharepointObject) object;
        return ClientUtil.downloadDriveItem(
                client, sharepointObject.getDriveId(), sharepointObject.getItem());
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
