package ai.langstream.ai.agents.commons.state;

import static ai.langstream.api.util.ConfigurationUtils.getBoolean;
import static ai.langstream.api.util.ConfigurationUtils.getString;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class LocalDiskStateStorage<T> implements StateStorage<T> {

    public static String computePath(
            final String tenant,
            final String globalAgentId,
            final Map<String, Object> agentConfiguration,
            final String suffix) {
        final boolean prependTenant =
                getBoolean("state-storage-file-prepend-tenant", false, agentConfiguration);
        final String prefix = getString("state-storage-file-prefix", "", agentConfiguration);

        final String pathPrefix;
        if (prependTenant) {
            pathPrefix = prefix + tenant + "-" + globalAgentId;
        } else {
            pathPrefix = prefix + globalAgentId;
        }
        return pathPrefix + "." + suffix + ".status.json";
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path path;

    @Override
    public void store(T status) throws Exception {
        log.info("Storing state to the disk at path {}", path);
        MAPPER.writeValue(path.toFile(), status);
    }

    @Override
    public T get(Class<T> clazz) throws Exception {
        if (Files.exists(path)) {
            log.info("Restoring state from {}", path);
            try {
                return MAPPER.readValue(path.toFile(), clazz);
            } catch (IOException e) {
                log.error("Error parsing state file", e);
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    public String getStateReference() {
        return path.toString();
    }
}
