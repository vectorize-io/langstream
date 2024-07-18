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

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertFalse;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runner.assets.AssetManagerRegistry;
import ai.langstream.api.runner.code.*;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.*;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.DeployContext;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.deployer.k8s.agents.AgentResourcesFactory;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.k8s.tests.KubeTestServer;
import ai.langstream.impl.nar.NarFileHandler;
import ai.langstream.impl.parser.ModelBuilder;
import ai.langstream.runtime.agent.AgentRunner;
import ai.langstream.runtime.agent.api.AgentAPIController;
import ai.langstream.runtime.api.agent.RuntimePodConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.Image;
import io.fabric8.kubernetes.api.model.Secret;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.*;
import org.opentest4j.AssertionFailedError;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public abstract class AbstractApplicationRunner {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final String INTEGRATION_TESTS_GROUP1 = "group-1";

    private static final int DEFAULT_NUM_LOOPS = 10;
    public static final Path agentsDirectory;

    static {
        agentsDirectory = Path.of(System.getProperty("user.dir"), "target", "agents");
        log.info("Agents directory is {}", agentsDirectory);
    }

    @RegisterExtension protected static final KubeTestServer kubeServer = new KubeTestServer();

    protected static ApplicationDeployer applicationDeployer;
    private static NarFileHandler narFileHandler;
    protected static TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry;
    @Getter private static Path basePersistenceDirectory;

    protected static Path codeDirectory;

    private static final Set<String> disposableImages = new HashSet<>();

    private int maxNumLoops = DEFAULT_NUM_LOOPS;
    private volatile boolean validateConsumerOffsets = true;

    public void setValidateConsumerOffsets(boolean validateConsumerOffsets) {
        this.validateConsumerOffsets = validateConsumerOffsets;
    }

    public int getMaxNumLoops() {
        return maxNumLoops;
    }

    public void setMaxNumLoops(int maxNumLoops) {
        this.maxNumLoops = maxNumLoops;
    }

    protected record ApplicationRuntime(
            String tenant,
            String applicationId,
            Application applicationInstance,
            ExecutionPlan implementation,
            Map<String, Secret> secrets)
            implements AutoCloseable {

        public <T> T getGlobal(String key) {
            return (T) implementation.getApplication().getInstance().globals().get(key);
        }

        public void close() {
            applicationDeployer.cleanup(tenant, implementation, codeDirectory);
            applicationDeployer.delete(tenant, implementation, null);
            Awaitility.await()
                    .until(
                            () -> {
                                log.info("Waiting for secrets to be deleted. {}", secrets);
                                return secrets.isEmpty();
                            });
        }
    }

    protected ApplicationRuntime deployApplication(
            String tenant,
            String appId,
            Map<String, String> application,
            String instance,
            String... expectedAgents)
            throws Exception {
        return deployApplicationWithSecrets(
                tenant, appId, application, instance, null, expectedAgents);
    }

    protected ApplicationRuntime deployApplicationWithSecrets(
            String tenant,
            String appId,
            Map<String, String> application,
            String instance,
            String secretsContents,
            String... expectedAgents)
            throws Exception {

        kubeServer.spyAgentCustomResources(tenant, expectedAgents);
        final Map<String, Secret> secrets =
                kubeServer.spyAgentCustomResourcesSecrets(tenant, expectedAgents);

        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(application, instance, secretsContents)
                        .getApplication();

        ExecutionPlan implementation =
                applicationDeployer.createImplementation(appId, applicationInstance);

        applicationDeployer.setup(tenant, implementation);
        applicationDeployer.deploy(tenant, implementation, null);

        return new ApplicationRuntime(tenant, appId, applicationInstance, implementation, secrets);
    }

    @BeforeAll
    public static void setup() throws Exception {
        codeDirectory = Paths.get("target/test-jdbc-drivers");
        basePersistenceDirectory =
                Files.createTempDirectory("langstream-agents-tests-persistent-state");
        narFileHandler =
                new NarFileHandler(
                        agentsDirectory,
                        AgentRunner.buildCustomLibClasspath(codeDirectory),
                        Thread.currentThread().getContextClassLoader());
        topicConnectionsRuntimeRegistry = new TopicConnectionsRuntimeRegistry();
        narFileHandler.scan();
        topicConnectionsRuntimeRegistry.setPackageLoader(narFileHandler);
        final AssetManagerRegistry assetManagerRegistry = new AssetManagerRegistry();
        assetManagerRegistry.setAssetManagerPackageLoader(narFileHandler);
        AgentCodeRegistry agentCodeRegistry = new AgentCodeRegistry();
        agentCodeRegistry.setAgentPackageLoader(narFileHandler);
        applicationDeployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .deployContext(DeployContext.NO_DEPLOY_CONTEXT)
                        .topicConnectionsRuntimeRegistry(topicConnectionsRuntimeRegistry)
                        .assetManagerRegistry(assetManagerRegistry)
                        .agentCodeRegistry(agentCodeRegistry)
                        .build();
    }

    public record AgentRunResult(Map<String, AgentAPIController> info) {}

    protected AgentRunResult executeAgentRunners(ApplicationRuntime runtime) throws Exception {
        String runnerExecutionId = UUID.randomUUID().toString();
        log.info(
                "{} Starting Agent Runners. Running {} pods",
                runnerExecutionId,
                runtime.secrets.size());
        Map<String, AgentAPIController> allAgentsInfo = new ConcurrentHashMap<>();
        try {
            List<RuntimePodConfiguration> pods = new ArrayList<>();
            runtime.secrets()
                    .forEach(
                            (key, secret) -> {
                                RuntimePodConfiguration runtimePodConfiguration =
                                        AgentResourcesFactory.readRuntimePodConfigurationFromSecret(
                                                secret);
                                log.info(
                                        "{} Pod configuration {} = {}",
                                        runnerExecutionId,
                                        key,
                                        runtimePodConfiguration);
                                pods.add(runtimePodConfiguration);
                            });
            // execute all the pods
            ExecutorService executorService = Executors.newCachedThreadPool();
            List<CompletableFuture<?>> futures = new ArrayList<>();
            for (RuntimePodConfiguration podConfiguration : pods) {
                CompletableFuture<?> handle = new CompletableFuture<>();
                futures.add(handle);
                executorService.submit(
                        () -> {
                            String originalName = Thread.currentThread().getName();
                            Thread.currentThread()
                                    .setName(
                                            podConfiguration.agent().agentId()
                                                    + "runner-tid-"
                                                    + runnerExecutionId);
                            try {
                                log.info(
                                        "{} AgentPod {} Started",
                                        runnerExecutionId,
                                        podConfiguration.agent().agentId());
                                AgentAPIController agentAPIController = new AgentAPIController();
                                allAgentsInfo.put(
                                        podConfiguration.agent().agentId(), agentAPIController);
                                AtomicInteger numLoops = new AtomicInteger();
                                for (String agentWithDisk :
                                        podConfiguration.agent().agentsWithDisk()) {
                                    Path directory =
                                            basePersistenceDirectory.resolve(agentWithDisk);
                                    if (!Files.isDirectory(directory)) {
                                        log.info(
                                                "Provisioning directory {} for stateful agent {}",
                                                directory,
                                                agentWithDisk);
                                        Files.createDirectory(directory);
                                    }
                                }
                                AgentRunner.runAgent(
                                        podConfiguration,
                                        codeDirectory,
                                        agentsDirectory,
                                        basePersistenceDirectory,
                                        agentAPIController,
                                        () -> {
                                            log.info(
                                                    "Num loops {}/{}", numLoops.get(), maxNumLoops);
                                            return numLoops.incrementAndGet() <= maxNumLoops;
                                        },
                                        () -> {
                                            if (validateConsumerOffsets) {
                                                validateAgentInfoBeforeStop(agentAPIController);
                                            }
                                        },
                                        false,
                                        narFileHandler,
                                        MetricsReporter.DISABLED);
                                List<?> infos = agentAPIController.serveWorkerStatus();
                                log.info(
                                        "{} AgentPod {} AgentInfo {}",
                                        runnerExecutionId,
                                        podConfiguration.agent().agentId(),
                                        infos);
                                handle.complete(null);
                            } catch (Throwable error) {
                                log.error(
                                        "{} Error on AgentPod {}",
                                        runnerExecutionId,
                                        podConfiguration.agent().agentId(),
                                        error);
                                handle.completeExceptionally(error);
                            } finally {
                                log.info(
                                        "{} AgentPod {} finished",
                                        runnerExecutionId,
                                        podConfiguration.agent().agentId());
                                Thread.currentThread().setName(originalName);
                            }
                        });
            }
            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
            } catch (ExecutionException executionException) {
                log.error(
                        "Some error occurred while executing the agent",
                        executionException.getCause());
                // unwrap the exception in order to easily perform assertions
                if (executionException.getCause() instanceof Exception) {
                    throw (Exception) executionException.getCause();
                } else {
                    throw executionException;
                }
            }
            executorService.shutdown();
            assertTrue(
                    executorService.awaitTermination(1, TimeUnit.MINUTES),
                    "the pods didn't finish in time");
        } finally {
            log.info("{} Agent Runners Stopped", runnerExecutionId);
        }
        return new AgentRunResult(allAgentsInfo);
    }

    public static DockerImageName markAsDisposableImage(DockerImageName dockerImageName) {
        disposableImages.add(dockerImageName.asCanonicalNameString());
        return dockerImageName;
    }

    @AfterEach
    @SneakyThrows
    public void resetNumLoops() {
        setMaxNumLoops(DEFAULT_NUM_LOOPS);
    }

    private static void dumpFsStats() {
        for (Path root : FileSystems.getDefault().getRootDirectories()) {
            try {
                FileStore store = Files.getFileStore(root);
                log.info(
                        "fs stats {}: available {}, total {}",
                        root,
                        FileUtils.byteCountToDisplaySize(store.getUsableSpace()),
                        FileUtils.byteCountToDisplaySize(store.getTotalSpace()));
            } catch (IOException e) {
                log.error("Error getting fs stats for {}", root, e);
            }
        }
        List<Image> images = DockerClientFactory.lazyClient().listImagesCmd().exec();
        for (Image image : images) {
            String size = FileUtils.byteCountToDisplaySize(image.getSize());
            if (image.getRepoTags() == null) {
                log.info("Docker dangling image size {}", size);
            } else {
                log.info("Docker image {} {} size {}", image.getId(), image.getRepoTags()[0], size);
            }
        }
    }

    @AfterAll
    public static void teardown() throws Exception {
        if (applicationDeployer != null) {
            // this closes the kubernetes client
            applicationDeployer.close();
        }
        if (narFileHandler != null) {
            narFileHandler.close();
        }
        if ("true".equalsIgnoreCase(System.getenv().get("CI"))) {
            dumpFsStats();
            for (String disposableImage : disposableImages) {
                try {
                    log.info("Removing image to save space on ci {}", disposableImage);
                    DockerClientFactory.lazyClient()
                            .removeImageCmd(disposableImage)
                            .withForce(true)
                            .exec();
                } catch (Exception e) {
                    log.error("Error removing image {}", disposableImage, e);
                }
            }
            dumpFsStats();
        }
        disposableImages.clear();
    }

    @SneakyThrows
    protected String buildInstanceYaml() {
        StreamingCluster streamingCluster = getStreamingCluster();
        String inputTopic = "input-topic-" + UUID.randomUUID();
        String outputTopic = "output-topic-" + UUID.randomUUID();
        String streamTopic = "stream-topic-" + UUID.randomUUID();
        return """
                instance:
                  globals:
                    input-topic: %s
                    output-topic: %s
                    stream-topic: %s
                  streamingCluster: %s
                  computeCluster:
                     type: "kubernetes"
                """
                .formatted(
                        inputTopic,
                        outputTopic,
                        streamTopic,
                        OBJECT_MAPPER.writeValueAsString(streamingCluster));
    }

    @SneakyThrows
    protected void sendFullMessage(
            TopicProducer producer,
            Object key,
            Object content,
            Collection<ai.langstream.api.runner.code.Header> headers) {
        if (content instanceof TopicProducer) {
            throw new UnsupportedOperationException("Cannot send a producer as content");
        }
        producer.write(SimpleRecord.builder().key(key).value(content).headers(headers).build())
                .get();
    }

    protected void sendMessageWithKey(TopicProducer producer, Object key, Object content) {
        sendFullMessage(producer, key, content, List.of());
    }

    protected void sendMessageWithHeaders(
            TopicProducer producer,
            Object content,
            Collection<ai.langstream.api.runner.code.Header> headers) {
        sendFullMessage(producer, null, content, headers);
    }

    protected void sendMessage(TopicProducer producer, Object content) {
        sendFullMessage(producer, null, content, List.of());
    }

    protected List<Record> waitForMessages(
            TopicConsumer consumer,
            BiConsumer<List<Record>, List<Object>> assertionOnReceivedMessages) {
        List<Record> result = new ArrayList<>();
        List<Object> received = new ArrayList<>();

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<Record> records = consumer.read();
                            for (Record record : records) {
                                log.info("Received message {}", record);
                                received.add(record.value());
                                result.add(record);
                            }
                            log.info("Result:  {}", received);
                            received.forEach(r -> log.info("Received |{}|", r));

                            try {
                                assertionOnReceivedMessages.accept(result, received);
                            } catch (AssertionFailedError assertionFailedError) {
                                log.info("Assertion failed", assertionFailedError);
                                throw assertionFailedError;
                            }
                        });

        return result;
    }

    protected List<Record> waitForMessages(TopicConsumer consumer, List<?> expected) {
        return waitForMessages(
                consumer,
                (result, received) -> {
                    assertEquals(expected.size(), received.size());
                    for (int i = 0; i < expected.size(); i++) {
                        Object expectedValue = expected.get(i);
                        Object actualValue = received.get(i);
                        if (expectedValue instanceof Consumer fn) {
                            fn.accept(actualValue);
                        } else if (expectedValue instanceof byte[]) {
                            assertArrayEquals((byte[]) expectedValue, (byte[]) actualValue);
                        } else {
                            log.info("expected: {}", expectedValue);
                            log.info("got: {}", actualValue);
                            assertEquals(expectedValue, actualValue);
                        }
                    }
                });
    }

    protected List<Record> waitForMessagesInAnyOrder(
            TopicConsumer consumer, Collection<String> expected) {
        List<Record> result = new ArrayList<>();
        List<Object> received = new ArrayList<>();

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<Record> records = consumer.read();
                            for (Record record : records) {
                                log.info("Received message {}", record);
                                received.add(record.value());
                                result.add(record);
                            }
                            consumer.commit(records);
                            log.info("Result: {}", received);
                            received.forEach(r -> log.info("Received |{}|", r));

                            assertEquals(expected.size(), received.size());
                            for (Object expectedValue : expected) {
                                // this doesn't work for byte[]
                                assertFalse(expectedValue instanceof byte[]);
                                assertTrue(
                                        received.contains(expectedValue),
                                        "Expected value "
                                                + expectedValue
                                                + " not found in "
                                                + received);
                            }

                            for (Object receivedValue : received) {
                                // this doesn't work for byte[]
                                assertFalse(receivedValue instanceof byte[]);
                                assertTrue(
                                        expected.contains(receivedValue),
                                        "Received value "
                                                + receivedValue
                                                + " not found in "
                                                + expected);
                            }
                        });

        return result;
    }

    protected static void assertRecordEquals(
            Record record, Object key, Object value, Map<String, String> headers) {
        final Object recordKey = record.key();
        final Object recordValue = record.value();
        Map<String, String> recordHeaders = new HashMap<>();
        for (Header header : record.headers()) {
            recordHeaders.put(header.key(), header.valueAsString());
        }

        log.info(
                """
                Comparing record with:
                key: {}
                value: {}
                headers: {}

                vs expected:
                key: {}
                value: {}
                headers: {}
                """,
                recordKey,
                recordValue,
                recordHeaders,
                key,
                value,
                headers);
        assertEquals(key, record.key());
        assertEquals(value, record.value());
        assertEquals(headers, recordHeaders);
    }

    protected abstract StreamingCluster getStreamingCluster();

    protected abstract void validateAgentInfoBeforeStop(AgentAPIController agentAPIController);
}
