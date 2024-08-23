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

import ai.langstream.api.doc.ConfigProperty;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.Data;

@Data
public class StorageProviderSourceBaseConfiguration {
    @ConfigProperty(
            description =
                    """
                    State storage type (s3, disk).
                    """)
    @JsonProperty("state-storage")
    private String stateStorage;

    @ConfigProperty(
            description =
                    """
                    Prepend tenant to the state storage file. (valid for all types)
                    """)
    @JsonProperty("state-storage-file-prepend-tenant")
    private String stateStorageFilePrependTenant;

    @ConfigProperty(
            description =
                    """
                    Prepend a prefix to the state storage file. (valid for all types)
                    """)
    @JsonProperty("state-storage-file-prefix")
    private String stateStorageFilePrefix;

    @ConfigProperty(
            description = """
                    State storage S3 bucket.
                    """)
    @JsonProperty("state-storage-s3-bucket")
    private String stateStorageS3Bucket;

    @ConfigProperty(
            description =
                    """
                    State storage S3 endpoint.
                    """)
    @JsonProperty("state-storage-s3-endpoint")
    private String stateStorageS3Endpoint;

    @ConfigProperty(
            description =
                    """
                    State storage S3 access key.
                    """)
    @JsonProperty("state-storage-s3-access-key")
    private String stateStorageS3AKey;

    @ConfigProperty(
            description =
                    """
                    State storage S3 secret key.
                    """)
    @JsonProperty("state-storage-s3-secret-key")
    private String stateStorageS3SecretKey;

    @ConfigProperty(
            description = """
                    State storage S3 region.
                    """)
    @JsonProperty("state-storage-s3-region")
    private String stateStorageS3Region;

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

    @ConfigProperty(
            description =
                    """
                            Additional headers to add to emitted records.
                            """)
    @JsonProperty("source-record-headers")
    private Map<String, String> sourceRecordHeaders;

    @ConfigProperty(
            defaultValue = "5",
            description =
                    """
                            Time in seconds to sleep after polling for new files.
                            """)
    @JsonProperty("idle-time")
    private int idleTime;
}
