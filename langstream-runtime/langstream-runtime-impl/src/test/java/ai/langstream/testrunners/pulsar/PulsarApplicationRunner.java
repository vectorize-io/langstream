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
package ai.langstream.testrunners.pulsar;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.runtime.agent.api.AgentAPIController;
import ai.langstream.testrunners.StreamingClusterRunner;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.ExtensionContext;

@Slf4j
public class PulsarApplicationRunner extends PulsarContainerExtension
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
        return Map.of("subscriptionName", "sub" + UUID.randomUUID(), "receiverQueueSize", 100);
    }

    @Override
    @SneakyThrows
    public Set<String> listTopics() {
        return getAdmin().topics().getList("public/default").stream()
                .map(t -> t.replace("persistent://public/default/", ""))
                .collect(Collectors.toSet());
    }

    @Override
    public StreamingCluster streamingCluster() {
        return new StreamingCluster(
                "pulsar",
                Map.of(
                        "admin",
                        Map.of("serviceUrl", getPulsarContainer().getHttpServiceUrl()),
                        "service",
                        Map.of("serviceUrl", getPulsarContainer().getPulsarBrokerUrl()),
                        "default-tenant",
                        "public",
                        "default-namespace",
                        "default"));
    }

    @Override
    public void validateAgentInfoBeforeStop(AgentAPIController agentAPIController) {}
}
