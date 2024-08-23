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

import ai.langstream.api.util.ObjectMapperFactory;
import io.minio.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class LocalDiskStateStorage<T> implements StateStorage<T> {

    public static Path computePath(
            Optional<Path> localDiskPath,
            final String tenant,
            final String globalAgentId,
            final Map<String, Object> agentConfiguration,
            final String agentId) {
        if (!localDiskPath.isPresent()) {
            throw new IllegalArgumentException(
                    "No local disk path available for agent "
                            + agentId
                            + " and state-storage was set to 'disk'");
        }
        final boolean prependTenant = StateStorage.isFilePrependTenant(agentConfiguration);
        final String prefix = StateStorage.getFilePrefix(agentConfiguration);

        final String pathPrefix;
        if (prependTenant) {
            pathPrefix = prefix + tenant + "-" + globalAgentId;
        } else {
            pathPrefix = prefix + globalAgentId;
        }
        return Path.of(
                localDiskPath.get().toFile().getAbsolutePath(),
                pathPrefix + "." + agentId + ".status.json");
    }

    private final Path path;

    @Override
    public void store(T status) throws Exception {
        log.info("Storing state to the disk at path {}", path);
        ObjectMapperFactory.getDefaultMapper().writeValue(path.toFile(), status);
    }

    @Override
    public T get(Class<T> clazz) throws Exception {
        if (Files.exists(path)) {
            log.info("Restoring state from {}", path);
            try {
                return ObjectMapperFactory.getDefaultMapper().readValue(path.toFile(), clazz);
            } catch (IOException e) {
                log.error("Error parsing state file", e);
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    public void delete() throws Exception {
        Files.deleteIfExists(path);
    }

    @Override
    public void close() throws Exception {}

    @Override
    public String getStateReference() {
        return path.toString();
    }
}
