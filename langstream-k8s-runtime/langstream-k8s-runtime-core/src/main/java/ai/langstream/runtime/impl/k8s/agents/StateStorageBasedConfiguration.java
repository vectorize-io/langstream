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
import lombok.Data;

@Data
public class StateStorageBasedConfiguration {
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
}
