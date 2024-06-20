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
package ai.langstream.ai.agents.commons.state;

import static ai.langstream.api.util.ConfigurationUtils.getString;

import io.minio.MinioClient;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StateStorageProvider<T> {

    public static final String CONFIGURATION_KEY_STATE_STORAGE = "state-storage";

    public StateStorage<T> create(
            final String tenant,
            final String agentId,
            final String globalAgentId,
            final Map<String, Object> agentConfiguration,
            Optional<Path> localDiskPath) {
        StateStorage<T> objectStateStorage =
                initStateStorage(tenant, agentId, globalAgentId, agentConfiguration, localDiskPath);
        if (objectStateStorage == null) {
            return null;
        }
        log.info(
                "State storage initialized for agent {} - type {} - reference {}",
                agentId,
                objectStateStorage.getClass().getSimpleName(),
                objectStateStorage.getStateReference());
        return objectStateStorage;
    }

    private static <T> StateStorage<T> initStateStorage(
            String tenant,
            String agentId,
            String globalAgentId,
            Map<String, Object> agentConfiguration,
            Optional<Path> localDiskPath) {
        final String stateStorage =
                getString(CONFIGURATION_KEY_STATE_STORAGE, "", agentConfiguration);
        if (stateStorage.isEmpty()) {
            return null;
        }

        if (stateStorage.equals("disk")) {

            log.info("Using local disk storage");

            Path stateFilename =
                    LocalDiskStateStorage.computePath(
                            localDiskPath, tenant, globalAgentId, agentConfiguration, agentId);

            return new LocalDiskStateStorage<>(stateFilename);
        } else {
            log.info("Using S3 storage");
            final String bucketName =
                    getString(
                            "state-storage-s3-bucket",
                            "langstream-s3-source-state",
                            agentConfiguration);
            final String endpoint =
                    getString(
                            "state-storage-s3-endpoint",
                            "http://minio-endpoint.-not-set:9090",
                            agentConfiguration);
            final String username =
                    getString("state-storage-s3-access-key", "minioadmin", agentConfiguration);
            final String password =
                    getString("state-storage-s3-secret-key", "minioadmin", agentConfiguration);
            final String region = getString("state-storage-s3-region", "", agentConfiguration);

            log.info(
                    "Connecting to S3 Bucket at {} in region {} with user {}",
                    endpoint,
                    region,
                    username);

            MinioClient.Builder builder =
                    MinioClient.builder().endpoint(endpoint).credentials(username, password);
            if (!region.isBlank()) {
                builder.region(region);
            }
            MinioClient minioClient = builder.build();
            String stateFilename =
                    S3StateStorage.computeObjectName(
                            tenant, globalAgentId, agentConfiguration, agentId);
            return new S3StateStorage<>(minioClient, bucketName, stateFilename);
        }
    }
}
