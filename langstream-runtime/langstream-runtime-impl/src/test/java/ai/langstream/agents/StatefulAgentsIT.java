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

import static ai.langstream.testrunners.AbstractApplicationRunner.INTEGRATION_TESTS_GROUP1;

import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.testrunners.AbstractGenericStreamingApplicationRunner;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Slf4j
@Tag(INTEGRATION_TESTS_GROUP1)
class StatefulAgentsIT extends AbstractGenericStreamingApplicationRunner {

    @Test
    public void testSingleStatefulAgent() throws Exception {
        String inputTopic = "input-topic-" + UUID.randomUUID();
        String outputTopic = "output-topic-" + UUID.randomUUID();
        String tenant = "tenant";
        String[] expectedAgents = {"app-step"};

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "some agent"
                                    id: "step"
                                    type: "mock-stateful-processor"
                                    input: "%s"
                                    output: "%s"
                                    resources:
                                       disk:
                                          enabled: true
                                """
                                .formatted(inputTopic, outputTopic, inputTopic, outputTopic));
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (TopicProducer producer = createProducer(inputTopic);
                    TopicConsumer consumer = createConsumer(outputTopic)) {

                sendMessage(producer, "a");
                sendMessage(producer, "b");

                executeAgentRunners(applicationRuntime);

                waitForMessages(consumer, List.of("a", "ab"));
            }
        }

        // run again
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (TopicProducer producer = createProducer(inputTopic);
                    TopicConsumer consumer = createConsumer(outputTopic)) {

                sendMessage(producer, "c");
                sendMessage(producer, "d");

                executeAgentRunners(applicationRuntime);

                // we are reading the output topic from the beginning,
                // we see the whole sequence
                waitForMessages(consumer, List.of("a", "ab", "abc", "abcd"));
            }
        }
    }
}
