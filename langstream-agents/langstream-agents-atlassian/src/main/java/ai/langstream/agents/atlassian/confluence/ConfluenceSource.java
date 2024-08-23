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
package ai.langstream.agents.atlassian.confluence;

import ai.langstream.agents.atlassian.confluence.client.ConfluencePage;
import ai.langstream.agents.atlassian.confluence.client.ConfluenceRestAPIClient;
import ai.langstream.agents.atlassian.confluence.client.ConfluenceSpace;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderObjectReference;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderSource;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderSourceState;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.util.ConfigurationUtils;
import com.dropbox.core.v2.files.*;

import java.util.*;
import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConfluenceSource extends StorageProviderSource<ConfluenceSource.ConfluenceSourceState> {

    public static class ConfluenceSourceState extends StorageProviderSourceState {}

    private ConfluenceRestAPIClient client;

    private List<String> spaces;
    private Set<String> rootParents;

    @Override
    public Class<ConfluenceSourceState> getStateClass() {
        return ConfluenceSourceState.class;
    }

    @Override
    public void initializeClientAndConfig(Map<String, Object> configuration) {
        String username =
                ConfigurationUtils.requiredField(
                        configuration, "username", () -> "confluence source");
        String apiToken =
                ConfigurationUtils.requiredField(
                        configuration, "api-token", () -> "confluence source");
        String domain =
                ConfigurationUtils.requiredField(
                        configuration, "domain", () -> "confluence source");
        client = new ConfluenceRestAPIClient(username, apiToken, domain);
        initializeConfig(configuration);
    }

    void initializeConfig(Map<String, Object> configuration) {
        spaces = ConfigurationUtils.getList("spaces", configuration);
        if (spaces.isEmpty()) {
            throw new IllegalArgumentException("At least one space (name or key) must be specified");
        }
        rootParents = ConfigurationUtils.getSet("root-parents", configuration);
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
        for (String space : spaces) {
            List<ConfluenceSpace> spacesForSpace = client.findSpaceByNameOrKeyOrId(space);
            if (spacesForSpace.isEmpty()) {
                log.error("Space {} not found, make sure you inserted the name or the key or the id of the space", space);
                continue;
            }
            for (ConfluenceSpace confluenceSpace : spacesForSpace) {
                log.info("Found space {}", confluenceSpace);
                int before = collect.size();
                client.visitSpacePages(confluenceSpace.id(), rootParents, confluencePage -> collect.add(new StorageProviderObjectReference() {
                    @Override
                    public String id() {
                        return confluencePage.id();
                    }

                    @Override
                    public long size() {
                        return -1;
                    }

                    @Override
                    public String contentDigest() {
                        return confluencePage.pageVersion();
                    }

                    @Override
                    public Collection<Header> additionalRecordHeaders() {
                        return List.of(
                                SimpleRecord.SimpleHeader.of("confluence-space-name", confluenceSpace.name()),
                                SimpleRecord.SimpleHeader.of("confluence-space-key", confluenceSpace.key()),
                                SimpleRecord.SimpleHeader.of("confluence-space-id", confluenceSpace.key()),
                                SimpleRecord.SimpleHeader.of("confluence-page-title", confluencePage.title())
                                );
                    }
                }));
                log.info("Found {} pages in space {}", collect.size() - before, confluenceSpace);
            }
        }
        return collect;
    }

    @Override
    public byte[] downloadObject(StorageProviderObjectReference object) throws Exception {
        try {
            return client.exportPage(object.id());
        } catch (Exception e) {
            log.error("Error downloading page {} ({})", object.id(), object.additionalRecordHeaders(), e);
            throw e;
        }
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
