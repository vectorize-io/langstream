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
package ai.langstream.agents.dropbox;

import ai.langstream.ai.agents.commons.storage.provider.StorageProviderObjectReference;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderSource;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderSourceState;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.util.ConfigurationUtils;
import com.dropbox.core.DbxDownloader;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.*;
import java.io.ByteArrayOutputStream;
import java.util.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class DropboxSource extends StorageProviderSource<DropboxSource.DropboxSourceState> {

    public static class DropboxSourceState extends StorageProviderSourceState {}

    private DbxClientV2 client;

    private String pathPrefix;
    private Set<String> extensions;

    @Override
    public Class<DropboxSourceState> getStateClass() {
        return DropboxSourceState.class;
    }

    @Override
    public void initializeClientAndConfig(Map<String, Object> configuration) {
        String accessToken =
                ConfigurationUtils.requiredField(
                        configuration, "access-token", () -> "dropbox source");
        String clientIdentifier =
                ConfigurationUtils.getString(
                        "client-identifier", "langstream-source", configuration);
        DbxRequestConfig config =
                DbxRequestConfig.newBuilder(clientIdentifier).withAutoRetryEnabled().build();
        client = new DbxClientV2(config, accessToken);
        initializeConfig(configuration);
    }

    void initializeConfig(Map<String, Object> configuration) {
        pathPrefix = configuration.getOrDefault("path-prefix", "").toString();
        if (StringUtils.isNotEmpty(pathPrefix) && pathPrefix.endsWith("/")) {
            pathPrefix = pathPrefix.substring(0, pathPrefix.length() - 1);
        }

        extensions = ConfigurationUtils.getSet("extensions", configuration);
        if (extensions.isEmpty()) {
            log.info("No extensions filter set, getting all files");
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
        collectFiles(pathPrefix, collect);
        log.info("Found {} files", collect.size());
        return collect;
    }

    private void collectFiles(String path, List<StorageProviderObjectReference> collect)
            throws Exception {
        log.debug("Listing path {}", path);
        ListFolderResult result = client.files().listFolder(path);
        while (true) {
            for (Metadata metadata : result.getEntries()) {
                if (metadata instanceof DeletedMetadata) {
                    continue;
                } else if (metadata instanceof FolderMetadata folder) {
                    collectFiles(folder.getPathDisplay(), collect);
                } else if (metadata instanceof FileMetadata file) {
                    if (log.isDebugEnabled()) {
                        log.debug("found file {}", file);
                    }
                    if (file.getContentHash() == null) {
                        log.warn("No content hash for file {}", file.getPathDisplay());
                        continue;
                    }
                    if (!extensions.isEmpty()) {
                        final String extension;
                        if (file.getName().contains(".")) {
                            extension =
                                    file.getName().substring(file.getName().lastIndexOf('.') + 1);
                        } else {
                            extension = "";
                        }
                        if (!extensions.contains(extension)) {
                            log.info(
                                    "Skipping file with extension {} (extension {})",
                                    file.getPathDisplay(),
                                    extension);
                            continue;
                        }
                    }
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Adding file {}, id {}, size {}, digest {}, path {}",
                                file.getName(),
                                file.getId(),
                                file.getSize(),
                                file.getContentHash(),
                                file.getPathDisplay());
                    }
                    collect.add(new DropboxObject(file));
                } else {
                    log.warn("Unknown metadata type {}", metadata);
                }
            }
            if (!result.getHasMore()) {
                break;
            }
            result = client.files().listFolderContinue(result.getCursor());
        }
    }

    @Override
    public byte[] downloadObject(StorageProviderObjectReference object) throws Exception {
        DropboxObject file = (DropboxObject) object;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            log.info("Downloading file {}", file.getFile().getPathDisplay());
            try (DbxDownloader<FileMetadata> downloader =
                    client.files().download(file.getFile().getPathDisplay()); ) {
                downloader.download(baos);
            }
            return baos.toByteArray();
        } catch (Exception e) {
            log.error("Error downloading file {}", file.getFile().getPathDisplay(), e);
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

    @AllArgsConstructor
    @Getter
    private static class DropboxObject implements StorageProviderObjectReference {
        private final FileMetadata file;

        @Override
        public String id() {
            return file.getId();
        }

        @Override
        public long size() {
            return file.getSize();
        }

        @Override
        public String contentDigest() {
            return file.getContentHash();
        }

        @Override
        public Collection<Header> additionalRecordHeaders() {
            return List.of(
                    SimpleRecord.SimpleHeader.of("dropbox-path", file.getPathDisplay()),
                    SimpleRecord.SimpleHeader.of("dropbox-filename", file.getName()));
        }
    }
}
