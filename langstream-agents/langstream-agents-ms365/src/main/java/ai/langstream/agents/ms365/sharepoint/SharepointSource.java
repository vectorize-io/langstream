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
import static ai.langstream.api.util.ConfigurationUtils.getInt;

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
import java.io.InputStream;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SharepointSource
        extends StorageProviderSource<SharepointSource.SharepointSourceState> {

    public static class SharepointSourceState extends StorageProviderSourceState {}

    private GraphServiceClient client;

    private List<String> includeOnlySites;

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
                collectItems(site.getId(), drive.getId(), rootItem, collect);
            }
            log.info(
                    "Found {} items in site {} ({})",
                    collect.size() - beforeLength,
                    site.getDisplayName(),
                    site.getId());
        }
        return collect;
    }

    @SneakyThrows
    private void collectItems(
            final String siteId,
            final String driveId,
            DriveItem parentItem,
            List<StorageProviderObjectReference> collect) {
        List<DriveItem> children =
                client.drives()
                        .byDriveId(driveId)
                        .items()
                        .byDriveItemId(parentItem.getId())
                        .children()
                        .get(
                                request -> {
                                    request.queryParameters.select =
                                            new String[] {
                                                "id", "name", "size", "cTag", "eTag", "file",
                                                "folder"
                                            };
                                    request.queryParameters.orderby = new String[] {"name asc"};
                                })
                        .getValue();
        if (children != null) {
            for (DriveItem item : children) {
                Objects.requireNonNull(item.getId());
                if (log.isDebugEnabled()) {
                    log.debug(
                            "File {} ({}), size {}, ctag {}, parents {}, last modified time {}",
                            item.getName(),
                            item.getId(),
                            item.getSize(),
                            item.getCTag());
                }
                if (item.getFolder() != null) {
                    log.debug("Folder {} ({})", item.getName(), item.getId());
                    collectItems(siteId, driveId, item, collect);
                } else {
                    log.debug("File {} ({})", item.getName(), item.getId());
                    File file = item.getFile();
                    Objects.requireNonNull(file);
                    if (!includeMimeTypes.isEmpty()
                            && !includeMimeTypes.contains(file.getMimeType())) {
                        log.debug(
                                "Skipping file {} ({}) due to mime type {}",
                                item.getName(),
                                item.getId(),
                                file.getMimeType());
                        continue;
                    }
                    if (!excludeMimeTypes.isEmpty()
                            && excludeMimeTypes.contains(file.getMimeType())) {
                        log.debug(
                                "Skipping file {} ({}) due to excluded mime type {}",
                                item.getName(),
                                item.getId(),
                                file.getMimeType());
                        continue;
                    }
                    final String digest;
                    if (item.getCTag() != null) {
                        digest = item.getCTag();
                    } else if (item.getETag() != null) {
                        log.warn(
                                "Using file eTag as digest for {}, this might be end up in duplicated processing",
                                item.getId());
                        digest = item.getETag();
                    } else {
                        log.error("Not able to compute a digest for {}, skipping", item.getId());
                        continue;
                    }
                    SharepointObject sharepointObject =
                            new SharepointObject(
                                    item.getId(),
                                    siteId,
                                    item.getSize() == null ? -1 : item.getSize(),
                                    digest,
                                    driveId,
                                    item.getName());
                    collect.add(sharepointObject);
                }
            }
        }
    }

    @AllArgsConstructor
    @Data
    private static class SharepointObject implements StorageProviderObjectReference {
        private final String itemId;
        private final String siteId;
        private final long size;
        private final String contentDigest;
        private final String driveId;
        private final String name;

        @Override
        public String id() {
            return siteId + "_" + itemId;
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public String contentDigest() {
            return contentDigest;
        }

        @Override
        public Collection<Header> additionalRecordHeaders() {
            return List.of(
                    SimpleRecord.SimpleHeader.of("sharepoint-item-name", name),
                    SimpleRecord.SimpleHeader.of("sharepoint-item-id", itemId),
                    SimpleRecord.SimpleHeader.of("sharepoint-drive-id", driveId),
                    SimpleRecord.SimpleHeader.of("sharepoint-site-id", siteId));
        }
    }

    @Override
    public byte[] downloadObject(StorageProviderObjectReference object) throws Exception {
        SharepointObject sharepointObject = (SharepointObject) object;
        try {
            try (InputStream in =
                    client.drives()
                            .byDriveId(sharepointObject.getDriveId())
                            .items()
                            .byDriveItemId(sharepointObject.getItemId())
                            .content()
                            .get(
                                    requestConfiguration -> {
                                        Objects.requireNonNull(
                                                requestConfiguration.queryParameters);
                                        requestConfiguration.queryParameters.format = "pdf";
                                    }); ) {
                if (in == null) {
                    throw new IllegalStateException(
                            "No content for file "
                                    + sharepointObject.getName()
                                    + " ("
                                    + sharepointObject.id()
                                    + ")");
                }
                return in.readAllBytes();
            }
        } catch (Exception e) {
            log.error(
                    "Error downloading file "
                            + sharepointObject.getName()
                            + " ("
                            + sharepointObject.id()
                            + ")",
                    e);
            throw e;
        }
    }

    @Override
    public void deleteObject(String id) throws Exception {
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
}
