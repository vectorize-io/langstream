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
package ai.langstream.testrunners.kafka;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.kafka.extensions.KafkaContainerExtension;
import ai.langstream.runtime.agent.api.AgentAPIController;
import ai.langstream.testrunners.StreamingClusterRunner;
import java.util.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.ExtensionContext;

@Slf4j
public class KafkaApplicationRunner extends KafkaContainerExtension
        implements StreamingClusterRunner {

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {}

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {}

    @Override
    public Map<String, Object> createProducerConfig() {
        return Map.of(
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
    }

    @Override
    public Map<String, Object> createConsumerConfig() {
        return Map.of(
                "group.id",
                "group-" + UUID.randomUUID(),
                "key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer",
                // poll 1 at time to align behaviour with Pulsar
                "max.poll.records",
                1);
    }

    @Override
    @SneakyThrows
    public Set<String> listTopics() {
        return new HashSet<>(getAdmin().listTopics().names().get());
    }

    @Override
    public StreamingCluster streamingCluster() {
        return new StreamingCluster(
                "kafka",
                Map.of(
                        "admin",
                        Map.of(
                                "bootstrap.servers",
                                getKafkaContainer().getBootstrapServers(),
                                "max.poll.records",
                                1)));
    }

    @Override
    public void validateAgentInfoBeforeStop(AgentAPIController agentAPIController) {
        agentAPIController
                .serveWorkerStatus()
                .forEach(
                        workerStatus -> {
                            String agentType = workerStatus.getAgentType();
                            log.info("Checking Agent type {}", agentType);
                            switch (agentType) {
                                case "topic-source":
                                    Map<String, Object> info = workerStatus.getInfo();
                                    log.info("Topic source info {}", info);
                                    Map<String, Object> consumerInfo =
                                            (Map<String, Object>) info.get("consumer");
                                    if (consumerInfo != null) {
                                        Map<String, Object> committedOffsets =
                                                (Map<String, Object>)
                                                        consumerInfo.get("committedOffsets");
                                        log.info("Committed offsets {}", committedOffsets);
                                        committedOffsets.forEach(
                                                (topic, offset) -> {
                                                    assertNotNull(offset);
                                                    assertTrue(((Number) offset).intValue() >= 0);
                                                });
                                        Map<String, Object> uncommittedOffsets =
                                                (Map<String, Object>)
                                                        consumerInfo.get("uncommittedOffsets");
                                        log.info("Uncommitted offsets {}", uncommittedOffsets);
                                        uncommittedOffsets.forEach(
                                                (topic, number) -> {
                                                    assertNotNull(number);
                                                    assertTrue(
                                                            ((Number) number).intValue() <= 0,
                                                            "for topic "
                                                                    + topic
                                                                    + " we have some uncommitted offsets: "
                                                                    + number);
                                                });
                                    }
                                default:
                                    // ignore
                            }
                        });
    }
}
