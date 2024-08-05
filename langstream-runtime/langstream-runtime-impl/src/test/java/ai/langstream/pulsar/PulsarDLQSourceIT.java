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
package ai.langstream.pulsar;

import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.testrunners.AbstractGenericStreamingApplicationRunner;
import ai.langstream.testrunners.pulsar.PulsarApplicationRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class PulsarDLQSourceIT extends AbstractGenericStreamingApplicationRunner {

    public PulsarDLQSourceIT() {
        super("pulsar");
    }

    @Test
    public void test() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1", "app-read-errors"};

        String serviceUrl = ((PulsarApplicationRunner) streamingClusterRunner).getBrokerUrl();

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "input"
                                    creation-mode: create-if-not-exists
                                  - name: "output"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "some agent"
                                    id: "step1"
                                    type: "mock-failing-processor"
                                    input: "input"
                                    output: "output"
                                    errors:
                                        on-failure: dead-letter
                                    configuration:
                                      fail-on-content: "fail-me"
                                """,
                        "pipeline-readerrors.yaml",
                        """
                                id: "read-errors"
                                topics:
                                - name: "input-deadletter"
                                  creation-mode: create-if-not-exists
                                - name: "dlq-out"
                                  creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "dlq"
                                    id: "read-errors"
                                    type: "pulsardlq-source"
                                    output: "dlq-out"
                                    configuration:
                                      pulsar-url: "%s"
                                      namespace: "public/default"
                                      subscription: "dlq-subscription"
                                      dlq-suffix: "-deadletter"
                                      include-partitioned: false
                                      timeout-ms: 1000
                                """.formatted(serviceUrl));
        try (ApplicationRuntime applicationRuntime =
                     deployApplication(
                             tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (TopicProducer producer = createProducer("input");
                 TopicConsumer consumer = createConsumer("output");
                 TopicConsumer consumerDeadletterOut = createConsumer("dlq-out")) {

                List<Object> expectedMessages = new ArrayList<>();
                List<Record> expectedMessagesDeadletter = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    sendMessage(producer, "fail-me-" + i);
                    sendMessage(producer, "keep-me-" + i);
                    expectedMessages.add("keep-me-" + i);
                    expectedMessagesDeadletter.add(SimpleRecord
                            .builder()
                            .value(("fail-me-" + i).getBytes(StandardCharsets.UTF_8))
                            .key(null)
                            .headers(List.of(
                                    SimpleRecord.SimpleHeader.of("langstream-error-type", "INTERNAL_ERROR"),
                                    SimpleRecord.SimpleHeader.of("cause-class", "ai.langstream.mockagents.MockProcessorAgentsCodeProvider$InjectedFailure"),
                                    SimpleRecord.SimpleHeader.of("langstream-error-cause-class", "ai.langstream.mockagents.MockProcessorAgentsCodeProvider$InjectedFailure"),
                                    SimpleRecord.SimpleHeader.of("error-class", "ai.langstream.runtime.agent.AgentRunner$PermanentFailureException"),
                                    SimpleRecord.SimpleHeader.of("langstream-error-class", "ai.langstream.runtime.agent.AgentRunner$PermanentFailureException"),
                                    SimpleRecord.SimpleHeader.of("source-topic", "persistent://public/default/input"),
                                    SimpleRecord.SimpleHeader.of("langstream-error-source-topic", "persistent://public/default/input"),
                                    SimpleRecord.SimpleHeader.of("cause-msg", "Failing on content: fail-me-" + i),
                                    SimpleRecord.SimpleHeader.of("langstream-error-cause-message", "Failing on content: fail-me-" + i),
                                    SimpleRecord.SimpleHeader.of("root-cause-class", "ai.langstream.mockagents.MockProcessorAgentsCodeProvider$InjectedFailure"),
                                    SimpleRecord.SimpleHeader.of("langstream-error-root-cause-class", "ai.langstream.mockagents.MockProcessorAgentsCodeProvider$InjectedFailure"),
                                    SimpleRecord.SimpleHeader.of("root-cause-msg", "Failing on content: fail-me-" + i),
                                    SimpleRecord.SimpleHeader.of("langstream-error-root-cause-message", "Failing on content: fail-me-" + i),
                                    SimpleRecord.SimpleHeader.of("error-msg", "ai.langstream.mockagents.MockProcessorAgentsCodeProvider$InjectedFailure: Failing on content: fail-me-" + i),
                                    SimpleRecord.SimpleHeader.of("langstream-error-message", "ai.langstream.mockagents.MockProcessorAgentsCodeProvider$InjectedFailure: Failing on content: fail-me-" + i)
                            ))
                            .build());
                }

                executeAgentRunners(applicationRuntime, 15);

                waitForRecords(consumerDeadletterOut, expectedMessagesDeadletter);
                waitForMessages(consumer, expectedMessages);
            }
        }
    }
}
