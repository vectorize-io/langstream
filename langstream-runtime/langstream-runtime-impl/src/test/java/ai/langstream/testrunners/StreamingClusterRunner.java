package ai.langstream.testrunners;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.runtime.agent.api.AgentAPIController;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;

public interface StreamingClusterRunner
        extends BeforeEachCallback, AfterEachCallback, AfterAllCallback, BeforeAllCallback {

    StreamingCluster streamingCluster();

    Map<String, Object> createProducerConfig();

    Map<String, Object> createConsumerConfig();

    Set<String> listTopics();

    void validateAgentInfoBeforeStop(AgentAPIController agentAPIController);
}
