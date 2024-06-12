package ai.langstream.ai.agents.commons.state;

import static ai.langstream.api.util.ConfigurationUtils.getBoolean;
import static ai.langstream.api.util.ConfigurationUtils.getString;

import java.util.Map;

public interface StateStorage<T> {

    static boolean isFilePrependTenant(Map<String, Object> agentConfiguration) {
        return getBoolean("state-storage-file-prepend-tenant", false, agentConfiguration);
    }

    static String getFilePrefix(Map<String, Object> agentConfiguration) {
        return getString("state-storage-file-prefix", "", agentConfiguration);
    }

    void store(T state) throws Exception;

    T get(Class<T> clazz) throws Exception;

    String getStateReference();
}
