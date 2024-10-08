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
package ai.langstream.deployer.k8s.util;

import ai.langstream.api.util.ObjectMapperFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

public class SerializationUtil {

    private static final ObjectMapper mapper = ObjectMapperFactory.getDefaultMapper();

    private static final ObjectMapper jsonPrettyPrint = ObjectMapperFactory.getPrettyPrintMapper();
    private static final ObjectMapper yamlMapper = ObjectMapperFactory.getDefaultYamlMapper();

    private SerializationUtil() {}

    @SneakyThrows
    public static String writeAsJson(Object object) {
        return mapper.writeValueAsString(object);
    }

    @SneakyThrows
    public static String prettyPrintJson(Object object) {
        return jsonPrettyPrint.writeValueAsString(object);
    }

    @SneakyThrows
    public static <T> T readJson(String string, Class<T> objectClass) {
        return mapper.readValue(string, objectClass);
    }

    @SneakyThrows
    public static byte[] writeAsJsonBytes(Object object) {
        return mapper.writeValueAsBytes(object);
    }

    @SneakyThrows
    public static String writeAsYaml(Object object) {
        return yamlMapper.writeValueAsString(object);
    }

    @SneakyThrows
    public static <T> T readYaml(String yaml, Class<T> toClass) {
        return yamlMapper.readValue(yaml, toClass);
    }

    public static String writeInlineBashJson(Object value) {
        return writeAsJson(value).replace("'", "'\"'\"'");
    }
}
