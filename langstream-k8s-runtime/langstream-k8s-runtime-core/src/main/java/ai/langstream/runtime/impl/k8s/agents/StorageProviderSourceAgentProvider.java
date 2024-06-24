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
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

/** Implements support for Storage provider source Agents. */
@Slf4j
public class StorageProviderSourceAgentProvider extends AbstractComposableAgentProvider {

    protected static final String AZURE_BLOB_STORAGE_SOURCE = "azure-blob-storage-source";
    protected static final String S3_SOURCE = "s3-source";
    protected static final String GCS_SOURCE = "google-cloud-storage-source";

    public StorageProviderSourceAgentProvider() {
        super(
                Set.of(S3_SOURCE, AZURE_BLOB_STORAGE_SOURCE, GCS_SOURCE),
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
            default:
                throw new IllegalArgumentException("Unknown agent type: " + type);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @AgentConfig(name = "S3 Source", description = "Reads data from S3 bucket")
    @Data
    public static class S3SourceConfiguration extends StateStorageBasedConfiguration {

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
                defaultValue = "5",
                description =
                        """
                                Time in seconds to sleep after polling for new files.
                                """)
        @JsonProperty("idle-time")
        private int idleTime;

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
                       Write a message to this topic when an object has been detected as deleted for any reason.
                                """)
        @JsonProperty("deleted-objects-topic")
        private String deletedObjectsTopic;

        @ConfigProperty(
                description =
                        """
                       Write a message to this topic periodically with a summary of the activity in the source. 
                                """)
        @JsonProperty("source-activity-summary-topic")
        private String sourceActivitySummaryTopic;

        @ConfigProperty(
                description =
                        """
                       List of events (comma separated) to include in the source activity summary. ('new', 'updated', 'deleted')
                       To include all: 'new,updated,deleted'.
                       Use this property to disable the source activity summary (by leaving default to empty).
                                """)
        @JsonProperty("source-activity-summary-events")
        private String sourceActivitySummaryEvents;
        @ConfigProperty(
                defaultValue = "60",
                description =
                        """
                        Trigger source activity summary emission when this number of events have been detected, even if the time threshold has not been reached yet.
                                """)
        @JsonProperty("source-activity-summary-events-threshold")
        private int sourceActivitySummaryNumEventsThreshold;

        @ConfigProperty(
                description =
                        """
                        Trigger source activity summary emission every time this time threshold has been reached.
                                """)
        @JsonProperty("source-activity-summary-time-seconds-threshold")
        private int sourceActivitySummaryTimeSecondsThreshold;
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
    public static class AzureBlobStorageConfiguration extends StateStorageBasedConfiguration {

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
                defaultValue = "5",
                description =
                        """
                Time in seconds to sleep after polling for new files.
                                """)
        @JsonProperty("idle-time")
        private int idleTime;

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
                       Write a message to this topic when an object has been detected as deleted for any reason.
                                """)
        @JsonProperty("deleted-objects-topic")
        private String deletedObjectsTopic;


        @ConfigProperty(
                description =
                        """
                       Write a message to this topic periodically with a summary of the activity in the source. 
                                """)
        @JsonProperty("source-activity-summary-topic")
        private String sourceActivitySummaryTopic;

        @ConfigProperty(
                description =
                        """
                       List of events (comma separated) to include in the source activity summary. ('new', 'updated', 'deleted')
                       To include all: 'new,updated,deleted'.
                       Use this property to disable the source activity summary (by leaving default to empty).
                                """)
        @JsonProperty("source-activity-summary-events")
        private String sourceActivitySummaryEvents;
        @ConfigProperty(
                defaultValue = "60",
                description =
                        """
                        Trigger source activity summary emission when this number of events have been detected, even if the time threshold has not been reached yet.
                                """)
        @JsonProperty("source-activity-summary-events-threshold")
        private int sourceActivitySummaryNumEventsThreshold;

        @ConfigProperty(
                description =
                        """
                        Trigger source activity summary emission every time this time threshold has been reached.
                                """)
        @JsonProperty("source-activity-summary-time-seconds-threshold")
        private int sourceActivitySummaryTimeSecondsThreshold;
    }

    @AgentConfig(
            name = "Google Cloud Storage Source",
            description =
                    """
    Reads data from Google Cloud Storage. The only authentication supported is via service account JSON.
    """)
    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class GoogleCloudStorageConfiguration extends StateStorageBasedConfiguration {

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
                defaultValue = "5",
                description =
                        """
                Time in seconds to sleep after polling for new files.
                                """)
        @JsonProperty("idle-time")
        private int idleTime;

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
                defaultValue = "true",
                description =
                        """
                       Write a message to this topic when an object has been detected as deleted for any reason.
                                """)
        @JsonProperty("deleted-objects-topic")
        private String deletedObjectsTopic;
    }
}
