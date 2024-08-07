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
package ai.langstream.api.model;

import ai.langstream.api.runtime.AgentNode;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;

@Data
public class AgentConfiguration {

    private String id;
    private String name;
    private String type;
    private Connection input;
    private Connection output;
    private Map<String, Object> configuration = new HashMap<>();
    private ResourcesSpec resources;
    private ErrorsSpec errors;
    private SignalsFromSpec signalsFrom;
    private AgentNode.DeletionMode deletionMode;
}
