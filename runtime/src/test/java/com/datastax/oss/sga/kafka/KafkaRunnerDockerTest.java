package com.datastax.oss.sga.kafka;

import com.dastastax.oss.sga.kafka.runtime.KafkaTopic;
import com.datastax.oss.sga.runtime.agent.AgentSpec;
import com.datastax.oss.sga.runtime.agent.PodJavaRuntime;
import com.datastax.oss.sga.runtime.agent.RuntimePodConfiguration;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Connection;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class KafkaRunnerDockerTest {

    private static KafkaContainer kafkaContainer;
    private static AdminClient admin;


    @Test
    public void testConnectToTopics() throws Exception {
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        buildInstanceYaml(),
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                """));

        ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module,
                new Connection(TopicDefinition.fromName("input-topic"))) instanceof KafkaTopic);

        deployer.deploy("tenant", implementation);

        Set<String> topics = admin.listTopics().names().get();
        log.info("Topics {}", topics);
        assertTrue(topics.contains("input-topic"));

        RuntimePodConfiguration runtimePodConfiguration = new RuntimePodConfiguration(
                Map.of("topic", "input-topic"),
                Map.of("topic", "output-topic"),
                new AgentSpec(AgentSpec.ComponentType.FUNCTION, "agent", "application", "identity", Map.of()),
                applicationInstance.getInstance().streamingCluster()
        );

        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(
                Map.of("bootstrap.servers", kafkaContainer.getBootstrapServers(),
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        );
                     KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(
                Map.of("bootstrap.servers", kafkaContainer.getBootstrapServers(),
                    "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                    "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                    "group.id","testgroup",
                    "auto.offset.reset", "earliest")
        )) {
            consumer.subscribe(List.of("output-topic"));


            // produce one message to the input-topic
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>("input-topic", "key", "value")).get();
            producer.flush();

            PodJavaRuntime.run(runtimePodConfiguration, 5);

            // receive one message from the output-topic (written by the PodJavaRuntime)
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(10));
            assertEquals(poll.count(), 1);
        }

    }

    private static String buildInstanceYaml() {
        return """
                instance:
                  streamingCluster:
                    type: "kafka"
                    configuration:
                      admin:
                        bootstrap.servers: "%s"
                  computeCluster:
                    type: "none"
                """.formatted(kafkaContainer.getBootstrapServers());
    }


    @BeforeAll
    public static void setup() throws Exception {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
                .withLogConsumer(new Consumer<OutputFrame>() {
                    @Override
                    public void accept(OutputFrame outputFrame) {
                        log.info("kafka> {}", outputFrame.getUtf8String().trim());
                    }
                });
        // start Pulsar and wait for it to be ready to accept requests
        kafkaContainer.start();
        admin =
                AdminClient.create(Map.of("bootstrap.servers", kafkaContainer.getBootstrapServers()));
    }

    @AfterAll
    public static void teardown() {
        if (admin != null) {
            admin.close();
        }
        if (kafkaContainer != null) {
            kafkaContainer.close();
        }
    }
}