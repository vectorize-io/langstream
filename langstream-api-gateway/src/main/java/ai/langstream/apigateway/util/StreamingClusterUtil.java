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
package ai.langstream.apigateway.util;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.util.ObjectMapperFactory;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;

public class StreamingClusterUtil {

    @SneakyThrows
    public static String asKey(StreamingCluster streamingCluster) {
        return ObjectMapperFactory.getDefaultMapper()
                .writeValueAsString(
                        Pair.of(streamingCluster.type(), streamingCluster.configuration()));
    }
}
