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
package ai.langstream.apigateway.api;

import lombok.AllArgsConstructor;

import java.util.Map;

public interface ProducePayload {

    ProduceRequest toProduceRequest();
    record ValuePayload(Object value) implements ProducePayload {
            @Override
            public Object key() {
                return null;
            }

            @Override
            public Map<String, String> headers() {
                return null;
            }

        @Override
        public ProduceRequest toProduceRequest() {
            return new ProduceRequest(null, value, null);
        }
    }
    Object key();
    Object value();
    Map<String, String> headers();
}
