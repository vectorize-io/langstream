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
package ai.langstream.testrunners.pravega;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.pravega.PravegaContainerExtension;
import ai.langstream.runtime.agent.api.AgentAPIController;
import ai.langstream.testrunners.StreamingClusterRunner;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.ExtensionContext;

@Slf4j
public class PravegaApplicationRunner extends PravegaContainerExtension
        implements StreamingClusterRunner {

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {}

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {}

    @Override
    public Map<String, Object> createProducerConfig() {
        return Map.of();
    }

    @Override
    public Map<String, Object> createConsumerConfig() {
        return Map.of("reader-group", "test-group" + UUID.randomUUID());
    }

    @Override
    @SneakyThrows
    public Set<String> listTopics() {

        throw new UnsupportedOperationException();
    }

    @Override
    public StreamingCluster streamingCluster() {
        return new StreamingCluster(
                "pravega",
                Map.of(
                        "client",
                        Map.of("scope", "langstream", "controller-uri", getControllerUri())));
    }

    @Override
    public void validateAgentInfoBeforeStop(AgentAPIController agentAPIController) {}
}
