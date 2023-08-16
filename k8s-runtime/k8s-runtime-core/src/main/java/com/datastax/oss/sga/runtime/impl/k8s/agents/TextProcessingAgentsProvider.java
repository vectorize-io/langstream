/**
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
package com.datastax.oss.sga.runtime.impl.k8s.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.impl.agents.AbstractComposableAgentProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;

import static com.datastax.oss.sga.runtime.impl.k8s.KubernetesClusterRuntime.CLUSTER_TYPE;

/**
 * Implements support for Text Processing Agents.
 */
@Slf4j
public class TextProcessingAgentsProvider extends AbstractComposableAgentProvider {

    private static final Set<String> SUPPORTED_AGENT_TYPES = Set.of("text-extractor",
            "language-detector",
            "text-splitter",
            "text-normaliser",
            "document-to-json");

    public TextProcessingAgentsProvider() {
        super(SUPPORTED_AGENT_TYPES, List.of(CLUSTER_TYPE, "none"));
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.PROCESSOR;
    }
}
