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
package ai.langstream;

import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.api.model.Application;
import ai.langstream.api.runner.assets.AssetManagerRegistry;
import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.code.MetricsReporter;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public abstract class AbstractApplicationRunner {

    private static final int DEDAULT_NUM_LOOPS = 5;
    public static final Path agentsDirectory;

    static {
        agentsDirectory = Path.of(System.getProperty("user.dir"), "target", "agents");
        log.info("Agents directory is {}", agentsDirectory);
    }

    @RegisterExtension protected static final KubeTestServer kubeServer = new KubeTestServer();

    protected static ApplicationDeployer applicationDeployer;
    private static NarFileHandler narFileHandler;
    @Getter private static Path basePersistenceDirectory;

    private static Path codeDirectory;

    private static final Set<String> disposableImages = new HashSet<>();

    private int maxNumLoops = DEDAULT_NUM_LOOPS;

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
            applicationDeployer.cleanup(tenant, implementation);
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
        TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry =
                new TopicConnectionsRuntimeRegistry();
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
                                        () -> validateAgentInfoBeforeStop(agentAPIController),
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

    protected void validateAgentInfoBeforeStop(AgentAPIController agentAPIController) {}

    public static DockerImageName markAsDisposableImage(DockerImageName dockerImageName) {
        disposableImages.add(dockerImageName.asCanonicalNameString());
        return dockerImageName;
    }

    @AfterEach
    public void resetNumLoops() {
        setMaxNumLoops(DEDAULT_NUM_LOOPS);
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
    public static void teardown() {
        if (applicationDeployer != null) {
            // this closes the kubernetes client
            applicationDeployer.close();
        }
        if (narFileHandler != null) {
            narFileHandler.close();
        }
        dumpFsStats();
        if ("true".equalsIgnoreCase(System.getenv().get("CI"))) {
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
        }
        disposableImages.clear();
        dumpFsStats();
    }
}
