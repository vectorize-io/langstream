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

import ai.langstream.api.doc.AgentConfigurationModel;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.impl.noop.NoOpComputeClusterRuntimeProvider;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class S3SourceAgentProviderTest {
    @Test
    @SneakyThrows
    public void testValidation() {
        validate(
                """
                        topics: []
                        pipeline:
                          - name: "s3-source"
                            type: "s3-source"
                            configuration:
                              a-field: "val"
                        """,
                "Found error on agent configuration (agent: 's3-source', type: 's3-source'). Property 'a-field' is unknown");
        validate(
                """
                        topics: []
                        pipeline:
                          - name: "s3-source"
                            type: "s3-source"
                            configuration: {}
                        """,
                null);
        validate(
                """
                        topics: []
                        pipeline:
                          - name: "s3-source"
                            type: "s3-source"
                            configuration:
                              bucketName: "my-bucket"
                              access-key: KK
                              secret-key: SS
                              endpoint: "http://localhost:9000"
                              idle-time: 0
                              region: "us-east-1"
                              file-extensions: "csv"
                        """,
                null);
        validate(
                """
                        topics: []
                        pipeline:
                          - name: "s3-source"
                            type: "s3-source"
                            configuration:
                              bucketName: 12
                        """,
                null);
        validate(
                """
                        topics: []
                        pipeline:
                          - name: "s3-source"
                            type: "s3-source"
                            configuration:
                              bucketName: {object: true}
                        """,
                "Found error on agent configuration (agent: 's3-source', type: 's3-source'). Property 'bucketName' has a wrong data type. Expected type: java.lang.String");
    }

    private void validate(String pipeline, String expectErrMessage) throws Exception {
        AgentValidationTestUtil.validate(pipeline, expectErrMessage);
    }

    @Test
    @SneakyThrows
    public void testDocumentation() {
        final Map<String, AgentConfigurationModel> model =
                new PluginsRegistry()
                        .lookupAgentImplementation(
                                "s3-source",
                                new NoOpComputeClusterRuntimeProvider.NoOpClusterRuntime())
                        .generateSupportedTypesDocumentation();

        Assertions.assertEquals(
                """
                        {
                          "azure-blob-storage-source" : {
                            "name" : "Azure Blob Storage Source",
                            "description" : "Reads data from Azure blobs. There are three supported ways to authenticate:\\n- SAS token\\n- Storage account name and key\\n- Storage account connection string",
                            "properties" : {
                              "container" : {
                                "description" : "The name of the Azure econtainer to read from.",
                                "required" : false,
                                "type" : "string",
                                "defaultValue" : "langstream-azure-source"
                              },
                              "delete-objects" : {
                                "description" : "Whether to delete objects after processing.",
                                "required" : false,
                                "type" : "boolean",
                                "defaultValue" : "true"
                              },
                              "deleted-objects-topic" : {
                                "description" : "Write a message to this topic when an object has been detected as deleted for any reason.",
                                "required" : false,
                                "type" : "string"
                              },
                              "endpoint" : {
                                "description" : "Endpoint to connect to. Usually it's https://<storage-account>.blob.core.windows.net.",
                                "required" : true,
                                "type" : "string"
                              },
                              "file-extensions" : {
                                "description" : "Comma separated list of file extensions to filter by.",
                                "required" : false,
                                "type" : "string",
                                "defaultValue" : "pdf,docx,html,htm,md,txt"
                              },
                              "idle-time" : {
                                "description" : "Time in seconds to sleep after polling for new files.",
                                "required" : false,
                                "type" : "integer",
                                "defaultValue" : "5"
                              },
                              "path-prefix" : {
                                "description" : "The prefix used to match objects when reading from the bucket.\\nDo not use a leading slash. To specify a directory, include a trailing slash.",
                                "required" : false,
                                "type" : "string"
                              },
                              "recursive" : {
                                "description" : "Flag to enable recursive directory following.",
                                "required" : false,
                                "type" : "boolean",
                                "defaultValue" : "false"
                              },
                              "sas-token" : {
                                "description" : "Azure SAS token. If not provided, storage account name and key must be provided.",
                                "required" : false,
                                "type" : "string"
                              },
                              "source-activity-summary-events" : {
                                "description" : "List of events (comma separated) to include in the source activity summary. ('new', 'updated', 'deleted')\\nTo include all: 'new,updated,deleted'.\\nUse this property to disable the source activity summary (by leaving default to empty).",
                                "required" : false,
                                "type" : "string"
                              },
                              "source-activity-summary-events-threshold" : {
                                "description" : "Trigger source activity summary emission when this number of events have been detected, even if the time threshold has not been reached yet.",
                                "required" : false,
                                "type" : "integer",
                                "defaultValue" : "60"
                              },
                              "source-activity-summary-time-seconds-threshold" : {
                                "description" : "Trigger source activity summary emission every time this time threshold has been reached.",
                                "required" : false,
                                "type" : "integer"
                              },
                              "source-activity-summary-topic" : {
                                "description" : "Write a message to this topic periodically with a summary of the activity in the source.",
                                "required" : false,
                                "type" : "string"
                              },
                              "source-record-headers" : {
                                "description" : "Additional headers to add to emitted records.",
                                "required" : false,
                                "type" : "object"
                              },
                              "state-storage" : {
                                "description" : "State storage type (s3, disk).",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-file-prefix" : {
                                "description" : "Prepend a prefix to the state storage file. (valid for all types)",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-file-prepend-tenant" : {
                                "description" : "Prepend tenant to the state storage file. (valid for all types)",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-access-key" : {
                                "description" : "State storage S3 access key.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-bucket" : {
                                "description" : "State storage S3 bucket.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-endpoint" : {
                                "description" : "State storage S3 endpoint.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-region" : {
                                "description" : "State storage S3 region.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-secret-key" : {
                                "description" : "State storage S3 secret key.",
                                "required" : false,
                                "type" : "string"
                              },
                              "storage-account-connection-string" : {
                                "description" : "Azure storage account connection string. If not provided, SAS token must be provided.",
                                "required" : false,
                                "type" : "string"
                              },
                              "storage-account-key" : {
                                "description" : "Azure storage account key. If not provided, SAS token must be provided.",
                                "required" : false,
                                "type" : "string"
                              },
                              "storage-account-name" : {
                                "description" : "Azure storage account name. If not provided, SAS token must be provided.",
                                "required" : false,
                                "type" : "string"
                              }
                            }
                          },
                          "dropbox-source" : {
                            "name" : "Dropbox Source",
                            "description" : "Reads data from Dropbox via an application.",
                            "properties" : {
                              "access-token" : {
                                "description" : "Access token for the Dropbox application.",
                                "required" : true,
                                "type" : "string"
                              },
                              "client-identifier" : {
                                "description" : "Entra MS registered application's tenant ID.",
                                "required" : false,
                                "type" : "string",
                                "defaultValue" : "langstream-source"
                              },
                              "deleted-objects-topic" : {
                                "description" : "Write a message to this topic when an object has been detected as deleted for any reason.",
                                "required" : false,
                                "type" : "string"
                              },
                              "extensions" : {
                                "description" : "Filter by file extension. By default, all files are included.",
                                "required" : false,
                                "type" : "array",
                                "items" : {
                                  "description" : "Filter by file extension. By default, all files are included.",
                                  "required" : false,
                                  "type" : "string"
                                }
                              },
                              "idle-time" : {
                                "description" : "Time in seconds to sleep after polling for new files.",
                                "required" : false,
                                "type" : "integer",
                                "defaultValue" : "5"
                              },
                              "path-prefix" : {
                                "description" : "The root directory to read from.\\nUse a leading slash.\\nExamples: /my-root/ or /my-root/sub-folder",
                                "required" : false,
                                "type" : "string"
                              },
                              "source-activity-summary-events" : {
                                "description" : "List of events (comma separated) to include in the source activity summary. ('new', 'updated', 'deleted')\\nTo include all: 'new,updated,deleted'.\\nUse this property to disable the source activity summary (by leaving default to empty).",
                                "required" : false,
                                "type" : "string"
                              },
                              "source-activity-summary-events-threshold" : {
                                "description" : "Trigger source activity summary emission when this number of events have been detected, even if the time threshold has not been reached yet.",
                                "required" : false,
                                "type" : "integer",
                                "defaultValue" : "60"
                              },
                              "source-activity-summary-time-seconds-threshold" : {
                                "description" : "Trigger source activity summary emission every time this time threshold has been reached.",
                                "required" : false,
                                "type" : "integer"
                              },
                              "source-activity-summary-topic" : {
                                "description" : "Write a message to this topic periodically with a summary of the activity in the source.",
                                "required" : false,
                                "type" : "string"
                              },
                              "source-record-headers" : {
                                "description" : "Additional headers to add to emitted records.",
                                "required" : false,
                                "type" : "object"
                              },
                              "state-storage" : {
                                "description" : "State storage type (s3, disk).",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-file-prefix" : {
                                "description" : "Prepend a prefix to the state storage file. (valid for all types)",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-file-prepend-tenant" : {
                                "description" : "Prepend tenant to the state storage file. (valid for all types)",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-access-key" : {
                                "description" : "State storage S3 access key.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-bucket" : {
                                "description" : "State storage S3 bucket.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-endpoint" : {
                                "description" : "State storage S3 endpoint.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-region" : {
                                "description" : "State storage S3 region.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-secret-key" : {
                                "description" : "State storage S3 secret key.",
                                "required" : false,
                                "type" : "string"
                              }
                            }
                          },
                          "google-cloud-storage-source" : {
                            "name" : "Google Cloud Storage Source",
                            "description" : "Reads data from Google Cloud Storage. The only authentication supported is via service account JSON.",
                            "properties" : {
                              "bucket-name" : {
                                "description" : "The name of the bucket.",
                                "required" : false,
                                "type" : "string",
                                "defaultValue" : "langstream-gcs-source"
                              },
                              "delete-objects" : {
                                "description" : "Whether to delete objects after processing.",
                                "required" : false,
                                "type" : "boolean",
                                "defaultValue" : "true"
                              },
                              "deleted-objects-topic" : {
                                "description" : "Write a message to this topic when an object has been detected as deleted for any reason.",
                                "required" : false,
                                "type" : "string"
                              },
                              "file-extensions" : {
                                "description" : "Comma separated list of file extensions to filter by.",
                                "required" : false,
                                "type" : "string",
                                "defaultValue" : "pdf,docx,html,htm,md,txt"
                              },
                              "idle-time" : {
                                "description" : "Time in seconds to sleep after polling for new files.",
                                "required" : false,
                                "type" : "integer",
                                "defaultValue" : "5"
                              },
                              "path-prefix" : {
                                "description" : "The prefix used to match objects when reading from the bucket.\\nDo not use a leading slash. To specify a directory, include a trailing slash.",
                                "required" : false,
                                "type" : "string"
                              },
                              "recursive" : {
                                "description" : "Flag to enable recursive directory following.",
                                "required" : false,
                                "type" : "boolean",
                                "defaultValue" : "false"
                              },
                              "service-account-json" : {
                                "description" : "Textual Service Account JSON to authenticate with.",
                                "required" : true,
                                "type" : "string"
                              },
                              "source-activity-summary-events" : {
                                "description" : "List of events (comma separated) to include in the source activity summary. ('new', 'updated', 'deleted')\\nTo include all: 'new,updated,deleted'.\\nUse this property to disable the source activity summary (by leaving default to empty).",
                                "required" : false,
                                "type" : "string"
                              },
                              "source-activity-summary-events-threshold" : {
                                "description" : "Trigger source activity summary emission when this number of events have been detected, even if the time threshold has not been reached yet.",
                                "required" : false,
                                "type" : "integer",
                                "defaultValue" : "60"
                              },
                              "source-activity-summary-time-seconds-threshold" : {
                                "description" : "Trigger source activity summary emission every time this time threshold has been reached.",
                                "required" : false,
                                "type" : "integer"
                              },
                              "source-activity-summary-topic" : {
                                "description" : "Write a message to this topic periodically with a summary of the activity in the source.",
                                "required" : false,
                                "type" : "string"
                              },
                              "source-record-headers" : {
                                "description" : "Additional headers to add to emitted records.",
                                "required" : false,
                                "type" : "object"
                              },
                              "state-storage" : {
                                "description" : "State storage type (s3, disk).",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-file-prefix" : {
                                "description" : "Prepend a prefix to the state storage file. (valid for all types)",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-file-prepend-tenant" : {
                                "description" : "Prepend tenant to the state storage file. (valid for all types)",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-access-key" : {
                                "description" : "State storage S3 access key.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-bucket" : {
                                "description" : "State storage S3 bucket.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-endpoint" : {
                                "description" : "State storage S3 endpoint.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-region" : {
                                "description" : "State storage S3 region.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-secret-key" : {
                                "description" : "State storage S3 secret key.",
                                "required" : false,
                                "type" : "string"
                              }
                            }
                          },
                          "google-drive-source" : {
                            "name" : "Google Drive Source",
                            "description" : "Reads data from Google Drive. The only authentication supported is via service account JSON.",
                            "properties" : {
                              "deleted-objects-topic" : {
                                "description" : "Write a message to this topic when an object has been detected as deleted for any reason.",
                                "required" : false,
                                "type" : "string"
                              },
                              "exclude-mime-types" : {
                                "description" : "Filter out mime types. Comma separated list of mime types. Only files with different mime types will be processed.\\nNote that folders are always discarded.",
                                "required" : false,
                                "type" : "array",
                                "items" : {
                                  "description" : "Filter out mime types. Comma separated list of mime types. Only files with different mime types will be processed.\\nNote that folders are always discarded.",
                                  "required" : false,
                                  "type" : "string"
                                }
                              },
                              "idle-time" : {
                                "description" : "Time in seconds to sleep after polling for new files.",
                                "required" : false,
                                "type" : "integer",
                                "defaultValue" : "5"
                              },
                              "include-mime-types" : {
                                "description" : "Filter by mime types. Comma separated list of mime types. Only files with these mime types will be processed.",
                                "required" : false,
                                "type" : "array",
                                "items" : {
                                  "description" : "Filter by mime types. Comma separated list of mime types. Only files with these mime types will be processed.",
                                  "required" : false,
                                  "type" : "string"
                                }
                              },
                              "root-parents" : {
                                "description" : "Filter by parent folders. Comma separated list of folder IDs. Only children will be processed.",
                                "required" : false,
                                "type" : "array",
                                "items" : {
                                  "description" : "Filter by parent folders. Comma separated list of folder IDs. Only children will be processed.",
                                  "required" : false,
                                  "type" : "string"
                                }
                              },
                              "service-account-json" : {
                                "description" : "Textual Service Account JSON to authenticate with.",
                                "required" : true,
                                "type" : "string"
                              },
                              "source-activity-summary-events" : {
                                "description" : "List of events (comma separated) to include in the source activity summary. ('new', 'updated', 'deleted')\\nTo include all: 'new,updated,deleted'.\\nUse this property to disable the source activity summary (by leaving default to empty).",
                                "required" : false,
                                "type" : "string"
                              },
                              "source-activity-summary-events-threshold" : {
                                "description" : "Trigger source activity summary emission when this number of events have been detected, even if the time threshold has not been reached yet.",
                                "required" : false,
                                "type" : "integer",
                                "defaultValue" : "60"
                              },
                              "source-activity-summary-time-seconds-threshold" : {
                                "description" : "Trigger source activity summary emission every time this time threshold has been reached.",
                                "required" : false,
                                "type" : "integer"
                              },
                              "source-activity-summary-topic" : {
                                "description" : "Write a message to this topic periodically with a summary of the activity in the source.",
                                "required" : false,
                                "type" : "string"
                              },
                              "source-record-headers" : {
                                "description" : "Additional headers to add to emitted records.",
                                "required" : false,
                                "type" : "object"
                              },
                              "state-storage" : {
                                "description" : "State storage type (s3, disk).",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-file-prefix" : {
                                "description" : "Prepend a prefix to the state storage file. (valid for all types)",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-file-prepend-tenant" : {
                                "description" : "Prepend tenant to the state storage file. (valid for all types)",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-access-key" : {
                                "description" : "State storage S3 access key.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-bucket" : {
                                "description" : "State storage S3 bucket.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-endpoint" : {
                                "description" : "State storage S3 endpoint.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-region" : {
                                "description" : "State storage S3 region.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-secret-key" : {
                                "description" : "State storage S3 secret key.",
                                "required" : false,
                                "type" : "string"
                              }
                            }
                          },
                          "ms365-onedrive-source" : {
                            "name" : "MS 365 OneDrive Source",
                            "description" : "Reads data from MS 365 OneDrive documents. The only authentication supported is application credentials and client secret.\\nPermissions must be set as \\"Application permissions\\" in the registered application. (not Delegated)",
                            "properties" : {
                              "deleted-objects-topic" : {
                                "description" : "Write a message to this topic when an object has been detected as deleted for any reason.",
                                "required" : false,
                                "type" : "string"
                              },
                              "exclude-mime-types" : {
                                "description" : "Filter out mime types. Comma separated list of mime types. Only files with different mime types will be processed.\\nNote that folders are always discarded.",
                                "required" : false,
                                "type" : "array",
                                "items" : {
                                  "description" : "Filter out mime types. Comma separated list of mime types. Only files with different mime types will be processed.\\nNote that folders are always discarded.",
                                  "required" : false,
                                  "type" : "string"
                                }
                              },
                              "idle-time" : {
                                "description" : "Time in seconds to sleep after polling for new files.",
                                "required" : false,
                                "type" : "integer",
                                "defaultValue" : "5"
                              },
                              "include-mime-types" : {
                                "description" : "Filter by mime types. Comma separated list of mime types. Only files with these mime types will be processed.",
                                "required" : false,
                                "type" : "array",
                                "items" : {
                                  "description" : "Filter by mime types. Comma separated list of mime types. Only files with these mime types will be processed.",
                                  "required" : false,
                                  "type" : "string"
                                }
                              },
                              "ms-client-id" : {
                                "description" : "Entra MS registered application ID (client ID).",
                                "required" : true,
                                "type" : "string"
                              },
                              "ms-client-secret" : {
                                "description" : "Entra MS registered application's client secret value.",
                                "required" : true,
                                "type" : "string"
                              },
                              "ms-tenant-id" : {
                                "description" : "Entra MS registered application's tenant ID.",
                                "required" : true,
                                "type" : "string"
                              },
                              "path-prefix" : {
                                "description" : "The root directory to read from.\\nDo not use a leading slash. To specify a directory, include a trailing slash.",
                                "required" : false,
                                "type" : "string",
                                "defaultValue" : "/"
                              },
                              "source-activity-summary-events" : {
                                "description" : "List of events (comma separated) to include in the source activity summary. ('new', 'updated', 'deleted')\\nTo include all: 'new,updated,deleted'.\\nUse this property to disable the source activity summary (by leaving default to empty).",
                                "required" : false,
                                "type" : "string"
                              },
                              "source-activity-summary-events-threshold" : {
                                "description" : "Trigger source activity summary emission when this number of events have been detected, even if the time threshold has not been reached yet.",
                                "required" : false,
                                "type" : "integer",
                                "defaultValue" : "60"
                              },
                              "source-activity-summary-time-seconds-threshold" : {
                                "description" : "Trigger source activity summary emission every time this time threshold has been reached.",
                                "required" : false,
                                "type" : "integer"
                              },
                              "source-activity-summary-topic" : {
                                "description" : "Write a message to this topic periodically with a summary of the activity in the source.",
                                "required" : false,
                                "type" : "string"
                              },
                              "source-record-headers" : {
                                "description" : "Additional headers to add to emitted records.",
                                "required" : false,
                                "type" : "object"
                              },
                              "state-storage" : {
                                "description" : "State storage type (s3, disk).",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-file-prefix" : {
                                "description" : "Prepend a prefix to the state storage file. (valid for all types)",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-file-prepend-tenant" : {
                                "description" : "Prepend tenant to the state storage file. (valid for all types)",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-access-key" : {
                                "description" : "State storage S3 access key.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-bucket" : {
                                "description" : "State storage S3 bucket.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-endpoint" : {
                                "description" : "State storage S3 endpoint.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-region" : {
                                "description" : "State storage S3 region.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-secret-key" : {
                                "description" : "State storage S3 secret key.",
                                "required" : false,
                                "type" : "string"
                              },
                              "users" : {
                                "description" : "Users drives to read from.",
                                "required" : true,
                                "type" : "array",
                                "items" : {
                                  "description" : "Users drives to read from.",
                                  "required" : true,
                                  "type" : "string"
                                }
                              }
                            }
                          },
                          "ms365-sharepoint-source" : {
                            "name" : "MS 365 Sharepoint Source",
                            "description" : "Reads data from MS 365 Sharepoint documents. The only authentication supported is application credentials and client secret.\\nPermissions must be set as \\"Application permissions\\" in the registered application. (not Delegated)",
                            "properties" : {
                              "deleted-objects-topic" : {
                                "description" : "Write a message to this topic when an object has been detected as deleted for any reason.",
                                "required" : false,
                                "type" : "string"
                              },
                              "exclude-mime-types" : {
                                "description" : "Filter out mime types. Comma separated list of mime types. Only files with different mime types will be processed.\\nNote that folders are always discarded.",
                                "required" : false,
                                "type" : "array",
                                "items" : {
                                  "description" : "Filter out mime types. Comma separated list of mime types. Only files with different mime types will be processed.\\nNote that folders are always discarded.",
                                  "required" : false,
                                  "type" : "string"
                                }
                              },
                              "idle-time" : {
                                "description" : "Time in seconds to sleep after polling for new files.",
                                "required" : false,
                                "type" : "integer",
                                "defaultValue" : "5"
                              },
                              "include-mime-types" : {
                                "description" : "Filter by mime types. Comma separated list of mime types. Only files with these mime types will be processed.",
                                "required" : false,
                                "type" : "array",
                                "items" : {
                                  "description" : "Filter by mime types. Comma separated list of mime types. Only files with these mime types will be processed.",
                                  "required" : false,
                                  "type" : "string"
                                }
                              },
                              "ms-client-id" : {
                                "description" : "Entra MS registered application ID (client ID).",
                                "required" : true,
                                "type" : "string"
                              },
                              "ms-client-secret" : {
                                "description" : "Entra MS registered application's client secret value.",
                                "required" : true,
                                "type" : "string"
                              },
                              "ms-tenant-id" : {
                                "description" : "Entra MS registered application's tenant ID.",
                                "required" : true,
                                "type" : "string"
                              },
                              "sites" : {
                                "description" : "Filter by sites. By default, all sites are included.",
                                "required" : false,
                                "type" : "array",
                                "items" : {
                                  "description" : "Filter by sites. By default, all sites are included.",
                                  "required" : false,
                                  "type" : "string"
                                }
                              },
                              "source-activity-summary-events" : {
                                "description" : "List of events (comma separated) to include in the source activity summary. ('new', 'updated', 'deleted')\\nTo include all: 'new,updated,deleted'.\\nUse this property to disable the source activity summary (by leaving default to empty).",
                                "required" : false,
                                "type" : "string"
                              },
                              "source-activity-summary-events-threshold" : {
                                "description" : "Trigger source activity summary emission when this number of events have been detected, even if the time threshold has not been reached yet.",
                                "required" : false,
                                "type" : "integer",
                                "defaultValue" : "60"
                              },
                              "source-activity-summary-time-seconds-threshold" : {
                                "description" : "Trigger source activity summary emission every time this time threshold has been reached.",
                                "required" : false,
                                "type" : "integer"
                              },
                              "source-activity-summary-topic" : {
                                "description" : "Write a message to this topic periodically with a summary of the activity in the source.",
                                "required" : false,
                                "type" : "string"
                              },
                              "source-record-headers" : {
                                "description" : "Additional headers to add to emitted records.",
                                "required" : false,
                                "type" : "object"
                              },
                              "state-storage" : {
                                "description" : "State storage type (s3, disk).",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-file-prefix" : {
                                "description" : "Prepend a prefix to the state storage file. (valid for all types)",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-file-prepend-tenant" : {
                                "description" : "Prepend tenant to the state storage file. (valid for all types)",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-access-key" : {
                                "description" : "State storage S3 access key.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-bucket" : {
                                "description" : "State storage S3 bucket.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-endpoint" : {
                                "description" : "State storage S3 endpoint.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-region" : {
                                "description" : "State storage S3 region.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-secret-key" : {
                                "description" : "State storage S3 secret key.",
                                "required" : false,
                                "type" : "string"
                              }
                            }
                          },
                          "s3-source" : {
                            "name" : "S3 Source",
                            "description" : "Reads data from S3 bucket",
                            "properties" : {
                              "access-key" : {
                                "description" : "Access key for the S3 server.",
                                "required" : false,
                                "type" : "string",
                                "defaultValue" : "minioadmin"
                              },
                              "bucketName" : {
                                "description" : "The name of the bucket to read from.",
                                "required" : false,
                                "type" : "string",
                                "defaultValue" : "langstream-source"
                              },
                              "delete-objects" : {
                                "description" : "Whether to delete objects after processing.",
                                "required" : false,
                                "type" : "boolean",
                                "defaultValue" : "true"
                              },
                              "deleted-objects-topic" : {
                                "description" : "Write a message to this topic when an object has been detected as deleted for any reason.",
                                "required" : false,
                                "type" : "string"
                              },
                              "endpoint" : {
                                "description" : "The endpoint of the S3 server.",
                                "required" : false,
                                "type" : "string",
                                "defaultValue" : "http://minio-endpoint.-not-set:9090"
                              },
                              "file-extensions" : {
                                "description" : "Comma separated list of file extensions to filter by.",
                                "required" : false,
                                "type" : "string",
                                "defaultValue" : "pdf,docx,html,htm,md,txt"
                              },
                              "idle-time" : {
                                "description" : "Time in seconds to sleep after polling for new files.",
                                "required" : false,
                                "type" : "integer",
                                "defaultValue" : "5"
                              },
                              "path-prefix" : {
                                "description" : "The prefix used to match objects when reading from the bucket.\\nDo not use a leading slash. To specify a directory, include a trailing slash.",
                                "required" : false,
                                "type" : "string"
                              },
                              "recursive" : {
                                "description" : "Flag to enable recursive directory following.",
                                "required" : false,
                                "type" : "boolean",
                                "defaultValue" : "false"
                              },
                              "region" : {
                                "description" : "Region for the S3 server.",
                                "required" : false,
                                "type" : "string"
                              },
                              "secret-key" : {
                                "description" : "Secret key for the S3 server.",
                                "required" : false,
                                "type" : "string",
                                "defaultValue" : "minioadmin"
                              },
                              "source-activity-summary-events" : {
                                "description" : "List of events (comma separated) to include in the source activity summary. ('new', 'updated', 'deleted')\\nTo include all: 'new,updated,deleted'.\\nUse this property to disable the source activity summary (by leaving default to empty).",
                                "required" : false,
                                "type" : "string"
                              },
                              "source-activity-summary-events-threshold" : {
                                "description" : "Trigger source activity summary emission when this number of events have been detected, even if the time threshold has not been reached yet.",
                                "required" : false,
                                "type" : "integer",
                                "defaultValue" : "60"
                              },
                              "source-activity-summary-time-seconds-threshold" : {
                                "description" : "Trigger source activity summary emission every time this time threshold has been reached.",
                                "required" : false,
                                "type" : "integer"
                              },
                              "source-activity-summary-topic" : {
                                "description" : "Write a message to this topic periodically with a summary of the activity in the source.",
                                "required" : false,
                                "type" : "string"
                              },
                              "source-record-headers" : {
                                "description" : "Additional headers to add to emitted records.",
                                "required" : false,
                                "type" : "object"
                              },
                              "state-storage" : {
                                "description" : "State storage type (s3, disk).",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-file-prefix" : {
                                "description" : "Prepend a prefix to the state storage file. (valid for all types)",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-file-prepend-tenant" : {
                                "description" : "Prepend tenant to the state storage file. (valid for all types)",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-access-key" : {
                                "description" : "State storage S3 access key.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-bucket" : {
                                "description" : "State storage S3 bucket.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-endpoint" : {
                                "description" : "State storage S3 endpoint.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-region" : {
                                "description" : "State storage S3 region.",
                                "required" : false,
                                "type" : "string"
                              },
                              "state-storage-s3-secret-key" : {
                                "description" : "State storage S3 secret key.",
                                "required" : false,
                                "type" : "string"
                              }
                            }
                          }
                        }""",
                SerializationUtil.prettyPrintJson(model));
    }
}
