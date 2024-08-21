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

import static ai.langstream.api.util.ConfigurationUtils.getBoolean;
import static ai.langstream.api.util.ConfigurationUtils.getString;

import java.util.Map;

public interface StateStorage<T> extends AutoCloseable {

    static boolean isFilePrependTenant(Map<String, Object> agentConfiguration) {
        return getBoolean("state-storage-file-prepend-tenant", false, agentConfiguration);
    }

    static String getFilePrefix(Map<String, Object> agentConfiguration) {
        return getString("state-storage-file-prefix", "", agentConfiguration);
    }

    void store(T state) throws Exception;

    T get(Class<T> clazz) throws Exception;

    void delete() throws Exception;

    String getStateReference();
}
