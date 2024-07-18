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
package ai.langstream.agents;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.api.runner.code.AgentStatusResponse;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.testrunners.AbstractGenericStreamingApplicationRunner;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class GenAIAgentsRunnerIT extends AbstractGenericStreamingApplicationRunner {

    @Test
    public void testRunAITools() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "drop-description"
                                    id: "step1"
                                    type: "drop-fields"
                                    input: "input-topic"
                                    output: "output-topic"
                                    configuration:
                                      fields:
                                        - "description"
                                """);

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {

            try (TopicProducer producer = createProducer("input-topic");
                    TopicConsumer consumer = createConsumer("output-topic")) {

                sendMessageWithHeaders(
                        producer,
                        "{\"name\": \"some name\", \"description\": \"some description\"}",
                        List.of(SimpleRecord.SimpleHeader.of("header-key", "header-value")));

                executeAgentRunners(applicationRuntime);

                List<Record> records =
                        waitForMessages(consumer, List.of("{\"name\":\"some name\"}"));

                Record record = records.get(0);
                assertEquals("{\"name\":\"some name\"}", record.value());
                assertEquals("header-value", record.getHeader("header-key").valueAsString());
            }
        }
    }

    @Test
    public void testRunAIToolsComposite() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "input-topic1"
                                    creation-mode: create-if-not-exists
                                  - name: "output-topic2"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "drop-description"
                                    id: "step1"
                                    type: "drop-fields"
                                    input: "input-topic1"
                                    configuration:
                                      fields:
                                        - "description"
                                  - name: "drop"
                                    id: "step2"
                                    type: "drop"
                                    output: "output-topic2"
                                """);

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {

            final AgentRunResult result = executeAgentRunners(applicationRuntime);

            final List<AgentStatusResponse> processors =
                    result.info().get("step1").serveWorkerStatus();
            assertEquals(4, processors.size());
            for (AgentStatusResponse p : processors) {
                boolean mayHaveProcessed = false;
                switch (p.getAgentId()) {
                    case "step1":
                        assertEquals("drop-fields", p.getAgentType());
                        break;
                    case "step2":
                        assertEquals("drop", p.getAgentType());
                        break;
                    case "topic-source":
                        // the topic source updates the lastProcessed
                        // even if it finds no messages in order to
                        // show that it is not stuck
                        mayHaveProcessed = true;
                        break;
                    case "topic-sink":
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + p.getAgentId());
                }
                assertTrue(p.getMetrics().getTotalIn() > 0);
                assertTrue(p.getMetrics().getTotalOut() > 0);
                if (!mayHaveProcessed) {
                    assertEquals(0L, p.getMetrics().getLastProcessedAt());
                }
                assertNotEquals(0L, p.getMetrics().getStartedAt());
            }
        }
    }
}
