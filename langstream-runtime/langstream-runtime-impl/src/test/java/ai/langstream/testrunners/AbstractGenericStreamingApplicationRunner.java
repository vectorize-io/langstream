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
package ai.langstream.testrunners;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.runtime.agent.api.AgentAPIController;
import ai.langstream.testrunners.kafka.KafkaApplicationRunner;
import ai.langstream.testrunners.pravega.PravegaApplicationRunner;
import ai.langstream.testrunners.pulsar.PulsarApplicationRunner;
import java.util.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

@Slf4j
public abstract class AbstractGenericStreamingApplicationRunner extends AbstractApplicationRunner {

    protected static StreamingClusterRunner streamingClusterRunner;
    private static TopicConnectionsRuntime topicConnectionsRuntime;
    private final String forceStreamingType;

    public AbstractGenericStreamingApplicationRunner() {
        this(null);
    }

    public AbstractGenericStreamingApplicationRunner(String forceStreamingType) {
        this.forceStreamingType = forceStreamingType;
    }

    @BeforeEach
    public void setupStreaming() throws Exception {
        String requestedType =
                forceStreamingType != null
                        ? forceStreamingType
                        : System.getenv("TESTS_RUNTIME_TYPE");
        if (requestedType == null) {
            requestedType = "kafka";
        }
        if (streamingClusterRunner == null
                || !streamingClusterRunner.streamingCluster().type().equals(requestedType)) {
            if (streamingClusterRunner != null) {
                resetClusterRunner();
            }
            switch (requestedType) {
                case "pulsar":
                    streamingClusterRunner = new PulsarApplicationRunner();
                    break;
                case "kafka":
                    streamingClusterRunner = new KafkaApplicationRunner();
                    break;
                case "pravega":
                    streamingClusterRunner = new PravegaApplicationRunner();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown streaming type: " + requestedType);
            }
            streamingClusterRunner.beforeAll(null);
            StreamingCluster streamingCluster = streamingClusterRunner.streamingCluster();
            if (streamingCluster != null) {
                topicConnectionsRuntime =
                        topicConnectionsRuntimeRegistry
                                .getTopicConnectionsRuntime(streamingCluster)
                                .asTopicConnectionsRuntime();
            }
        } else {
            streamingClusterRunner.beforeEach(null);
        }
    }

    @AfterEach
    public void streamingAfterEach() throws Exception {
        streamingClusterRunner.afterEach(null);
    }

    @AfterAll
    public static void streamingTeardown() throws Exception {
        resetClusterRunner();
    }

    private static void resetClusterRunner() throws Exception {
        if (topicConnectionsRuntime != null) {
            topicConnectionsRuntime.close();
            topicConnectionsRuntime = null;
        }
        if (streamingClusterRunner != null) {
            streamingClusterRunner.afterAll(null);
            streamingClusterRunner = null;
        }
    }

    @Override
    protected StreamingCluster getStreamingCluster() {
        return streamingClusterRunner.streamingCluster();
    }

    @SneakyThrows
    protected TopicProducer createProducer(String topic) {
        Map<String, Object> config = new HashMap<>(streamingClusterRunner.createProducerConfig());
        config.put("topic", topic);

        TopicProducer producer =
                topicConnectionsRuntime.createProducer(null, getStreamingCluster(), config);
        producer.start();
        return producer;
    }

    @SneakyThrows
    protected TopicConsumer createConsumer(String topic) {
        Map<String, Object> config = new HashMap<>(streamingClusterRunner.createConsumerConfig());
        config.put("topic", topic);
        TopicConsumer consumer =
                topicConnectionsRuntime.createConsumer(null, getStreamingCluster(), config);
        consumer.start();
        return consumer;
    }

    protected Set<String> listTopics() {
        return streamingClusterRunner.listTopics();
    }

    @Override
    protected void validateAgentInfoBeforeStop(AgentAPIController agentAPIController) {
        streamingClusterRunner.validateAgentInfoBeforeStop(agentAPIController);
    }
}
