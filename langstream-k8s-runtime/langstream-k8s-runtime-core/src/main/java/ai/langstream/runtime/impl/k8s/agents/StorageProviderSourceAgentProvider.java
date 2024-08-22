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
package ai.langstream.runtime.impl.k8s.agents;

import ai.langstream.api.doc.AgentConfig;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.impl.agents.AbstractComposableAgentProvider;
import ai.langstream.runtime.impl.k8s.KubernetesClusterRuntime;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

/**
 * Implements support for Storage provider source Agents.
 */
@Slf4j
public class StorageProviderSourceAgentProvider extends AbstractComposableAgentProvider {

    protected static final String AZURE_BLOB_STORAGE_SOURCE = "azure-blob-storage-source";
    protected static final String S3_SOURCE = "s3-source";
    protected static final String GCS_SOURCE = "google-cloud-storage-source";

    protected static final String GOOGLE_DRIVE_SOURCE = "google-drive-source";

    protected static final String MS_365_SHAREPOINT_SOURCE = "ms365-sharepoint-source";
    protected static final String MS_365_ONEDRIVE_SOURCE = "ms365-onedrive-source";

    public StorageProviderSourceAgentProvider() {
        super(
                Set.of(
                        S3_SOURCE,
                        AZURE_BLOB_STORAGE_SOURCE,
                        GCS_SOURCE,
                        GOOGLE_DRIVE_SOURCE,
                        MS_365_SHAREPOINT_SOURCE,
                        MS_365_ONEDRIVE_SOURCE),
                List.of(KubernetesClusterRuntime.CLUSTER_TYPE, "none"));
    }

    @Override
    protected final ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.SOURCE;
    }

    @Override
    protected Class getAgentConfigModelClass(String type) {
        switch (type) {
            case S3_SOURCE:
                return S3SourceConfiguration.class;
            case AZURE_BLOB_STORAGE_SOURCE:
                return AzureBlobStorageConfiguration.class;
            case GCS_SOURCE:
                return GoogleCloudStorageConfiguration.class;
            case GOOGLE_DRIVE_SOURCE:
                return GoogleDriveSourceConfiguration.class;
            case MS_365_SHAREPOINT_SOURCE:
                return MS365SharepointSourceConfiguration.class;
            case MS_365_ONEDRIVE_SOURCE:
                return MS365OneDriveSourceConfiguration.class;
            default:
                throw new IllegalArgumentException("Unknown agent type: " + type);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @AgentConfig(name = "S3 Source", description = "Reads data from S3 bucket")
    @Data
    public static class S3SourceConfiguration extends StorageProviderSourceBaseConfiguration {

        protected static final String DEFAULT_BUCKET_NAME = "langstream-source";
        protected static final String DEFAULT_ENDPOINT = "http://minio-endpoint.-not-set:9090";
        protected static final String DEFAULT_ACCESSKEY = "minioadmin";
        protected static final String DEFAULT_SECRETKEY = "minioadmin";
        protected static final String DEFAULT_FILE_EXTENSIONS = "pdf,docx,html,htm,md,txt";

        @ConfigProperty(
                description =
                        """
                                The name of the bucket to read from.
                                """,
                defaultValue = DEFAULT_BUCKET_NAME)
        private String bucketName = DEFAULT_BUCKET_NAME;

        @ConfigProperty(
                description =
                        """
                                The endpoint of the S3 server.
                                """,
                defaultValue = DEFAULT_ENDPOINT)
        private String endpoint = DEFAULT_ENDPOINT;

        @ConfigProperty(
                description =
                        """
                                Access key for the S3 server.
                                """,
                defaultValue = DEFAULT_ACCESSKEY)
        @JsonProperty("access-key")
        private String accessKey = DEFAULT_ACCESSKEY;

        @ConfigProperty(
                description =
                        """
                                Secret key for the S3 server.
                                """,
                defaultValue = DEFAULT_SECRETKEY)
        @JsonProperty("secret-key")
        private String secretKey = DEFAULT_SECRETKEY;

        @ConfigProperty(
                required = false,
                description =
                        """
                                Region for the S3 server.
                                """)
        private String region = "";


        @ConfigProperty(
                defaultValue = DEFAULT_FILE_EXTENSIONS,
                description =
                        """
                                Comma separated list of file extensions to filter by.
                                """)
        @JsonProperty("file-extensions")
        private String fileExtensions = DEFAULT_FILE_EXTENSIONS;

        @ConfigProperty(
                defaultValue = "true",
                description =
                        """
                                Whether to delete objects after processing.
                                         """)
        @JsonProperty("delete-objects")
        private boolean deleteObjects;


        @ConfigProperty(
                description =
                        """
                                The prefix used to match objects when reading from the bucket.
                                Do not use a leading slash. To specify a directory, include a trailing slash.                                 """)
        @JsonProperty("path-prefix")
        private String pathPrefix;

        @ConfigProperty(
                defaultValue = "false",
                description =
                        """
                                Flag to enable recursive directory following.
                                 """)
        @JsonProperty("recursive")
        private boolean recursive;
    }

    @AgentConfig(
            name = "Azure Blob Storage Source",
            description =
                    """
                            Reads data from Azure blobs. There are three supported ways to authenticate:
                            - SAS token
                            - Storage account name and key
                            - Storage account connection string
                            """)
    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class AzureBlobStorageConfiguration extends StorageProviderSourceBaseConfiguration {

        @ConfigProperty(
                defaultValue = "langstream-azure-source",
                description =
                        """
                                The name of the Azure econtainer to read from.
                                """)
        private String container;

        @ConfigProperty(
                required = true,
                description =
                        """
                                Endpoint to connect to. Usually it's https://<storage-account>.blob.core.windows.net.
                                """)
        private String endpoint;

        @ConfigProperty(
                description =
                        """
                                Azure SAS token. If not provided, storage account name and key must be provided.
                                """)
        @JsonProperty("sas-token")
        private String sasToken;

        @ConfigProperty(
                description =
                        """
                                Azure storage account name. If not provided, SAS token must be provided.
                                """)
        @JsonProperty("storage-account-name")
        private String storageAccountName;

        @ConfigProperty(
                description =
                        """
                                Azure storage account key. If not provided, SAS token must be provided.
                                """)
        @JsonProperty("storage-account-key")
        private String storageAccountKey;

        @ConfigProperty(
                description =
                        """
                                Azure storage account connection string. If not provided, SAS token must be provided.
                                """)
        @JsonProperty("storage-account-connection-string")
        private String storageAccountConnectionString;

        @ConfigProperty(
                defaultValue = "pdf,docx,html,htm,md,txt",
                description =
                        """
                                Comma separated list of file extensions to filter by.
                                """)
        @JsonProperty("file-extensions")
        private String fileExtensions;

        @ConfigProperty(
                defaultValue = "true",
                description =
                        """
                                Whether to delete objects after processing.
                                         """)
        @JsonProperty("delete-objects")
        private boolean deleteObjects;


        @ConfigProperty(
                description =
                        """
                                The prefix used to match objects when reading from the bucket.
                                Do not use a leading slash. To specify a directory, include a trailing slash.                                 """)
        @JsonProperty("path-prefix")
        private String pathPrefix;

        @ConfigProperty(
                defaultValue = "false",
                description =
                        """
                                Flag to enable recursive directory following.
                                 """)
        @JsonProperty("recursive")
        private boolean recursive;
    }

    @AgentConfig(
            name = "Google Cloud Storage Source",
            description =
                    """
                            Reads data from Google Cloud Storage. The only authentication supported is via service account JSON.
                            """)
    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class GoogleCloudStorageConfiguration extends StorageProviderSourceBaseConfiguration {

        @ConfigProperty(
                defaultValue = "langstream-gcs-source",
                description =
                        """
                                The name of the bucket.
                                """)
        @JsonProperty("bucket-name")
        private String bucketName;

        @ConfigProperty(
                required = true,
                description =
                        """
                                Textual Service Account JSON to authenticate with.
                                """)
        @JsonProperty("service-account-json")
        private String serviceAccountJson;


        @ConfigProperty(
                defaultValue = "pdf,docx,html,htm,md,txt",
                description =
                        """
                                Comma separated list of file extensions to filter by.
                                """)
        @JsonProperty("file-extensions")
        private String fileExtensions;

        @ConfigProperty(
                defaultValue = "true",
                description =
                        """
                                Whether to delete objects after processing.
                                         """)
        @JsonProperty("delete-objects")
        private boolean deleteObjects;

        @ConfigProperty(
                description =
                        """
                                The prefix used to match objects when reading from the bucket.
                                Do not use a leading slash. To specify a directory, include a trailing slash.                                 """)
        @JsonProperty("path-prefix")
        private String pathPrefix;

        @ConfigProperty(
                defaultValue = "false",
                description =
                        """
                                Flag to enable recursive directory following.
                                 """)
        @JsonProperty("recursive")
        private boolean recursive;
    }

    @AgentConfig(
            name = "Google Drive Source",
            description =
                    """
                            Reads data from Google Drive. The only authentication supported is via service account JSON.
                            """)
    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class GoogleDriveSourceConfiguration extends StorageProviderSourceBaseConfiguration {

        @ConfigProperty(
                required = true,
                description =
                        """
                                Textual Service Account JSON to authenticate with.
                                """)
        @JsonProperty("service-account-json")
        private String serviceAccountJson;

        @ConfigProperty(
                description =
                        """
                                Filter by parent folders. Comma separated list of folder IDs. Only children will be processed.
                                        """)
        @JsonProperty("root-parents")
        private List<String> rootParents;

        @ConfigProperty(
                description =
                        """
                                Filter by mime types. Comma separated list of mime types. Only files with these mime types will be processed.
                                        """)
        @JsonProperty("include-mime-types")
        private List<String> includeMimeTypes;

        @ConfigProperty(
                description =
                        """
                                Filter out mime types. Comma separated list of mime types. Only files with different mime types will be processed.
                                Note that folders are always discarded.
                                        """)
        @JsonProperty("exclude-mime-types")
        private List<String> excludeMimeTypes;

    }

    @AgentConfig(
            name = "MS 365 Sharepoint Source",
            description =
                    """
                            Reads data from MS 365 Sharepoint documents. The only authentication supported is application credentials and client secret.
                            Permissions must be set as "Application permissions" in the registered application. (not Delegated)
                            """)
    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class MS365SharepointSourceConfiguration extends StorageProviderSourceBaseConfiguration {

        @ConfigProperty(
                required = true,
                description =
                        """
                                Entra MS registered application ID (client ID).
                                """)
        @JsonProperty("ms-client-id")
        private String clientId;

        @ConfigProperty(
                required = true,
                description =
                        """
                                Entra MS registered application's tenant ID.
                                """)
        @JsonProperty("ms-tenant-id")
        private String tenantId;

        @ConfigProperty(
                required = true,
                description =
                        """
                                Entra MS registered application's client secret value.
                                """)
        @JsonProperty("ms-client-secret")
        private String clientSecret;

        @ConfigProperty(
                description =
                        """
                                Filter by sites. By default, all sites are included.
                                        """)
        @JsonProperty("sites")
        private List<String> sites;

        @ConfigProperty(
                description =
                        """
                                Filter by mime types. Comma separated list of mime types. Only files with these mime types will be processed.
                                        """)
        @JsonProperty("include-mime-types")
        private List<String> includeMimeTypes;

        @ConfigProperty(
                description =
                        """
                                Filter out mime types. Comma separated list of mime types. Only files with different mime types will be processed.
                                Note that folders are always discarded.
                                        """)
        @JsonProperty("exclude-mime-types")
        private List<String> excludeMimeTypes;

    }

    @AgentConfig(
            name = "MS 365 OneDrive Source",
            description =
                    """
                            Reads data from MS 365 OneDrive documents. The only authentication supported is application credentials and client secret.
                            Permissions must be set as "Application permissions" in the registered application. (not Delegated)
                            """)
    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class MS365OneDriveSourceConfiguration extends StorageProviderSourceBaseConfiguration {

        @ConfigProperty(
                required = true,
                description =
                        """
                                Entra MS registered application ID (client ID).
                                """)
        @JsonProperty("ms-client-id")
        private String clientId;

        @ConfigProperty(
                required = true,
                description =
                        """
                                Entra MS registered application's tenant ID.
                                """)
        @JsonProperty("ms-tenant-id")
        private String tenantId;

        @ConfigProperty(
                required = true,
                description =
                        """
                                Entra MS registered application's client secret value.
                                """)
        @JsonProperty("ms-client-secret")
        private String clientSecret;

        @ConfigProperty(
                required = true,
                description =
                        """
                                Users drives to read from.
                                        """)
        @JsonProperty("users")
        private List<String> users;

        @ConfigProperty(
                description =
                        """
                                Filter by mime types. Comma separated list of mime types. Only files with these mime types will be processed.
                                        """)
        @JsonProperty("include-mime-types")
        private List<String> includeMimeTypes;

        @ConfigProperty(
                description =
                        """
                                Filter out mime types. Comma separated list of mime types. Only files with different mime types will be processed.
                                Note that folders are always discarded.
                                        """)
        @JsonProperty("exclude-mime-types")
        private List<String> excludeMimeTypes;

        @ConfigProperty(
                description =
                        """
                                The root directory to read from.
                                Do not use a leading slash. To specify a directory, include a trailing slash.                                 """,
                defaultValue = "/")
        @JsonProperty("path-prefix")
        private String pathPrefix;

    }
}
