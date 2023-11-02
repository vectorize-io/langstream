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
package ai.langstream.pravega;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.Connection;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.parser.ModelBuilder;
import io.pravega.client.admin.StreamManager;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
class PravegaClusterRuntimeDockerTest {

    @RegisterExtension
    static final PravegaContainerExtension pravegaContainer = new PravegaContainerExtension();

    @Test
    public void testMapPravegaTopic() throws Exception {
        final StreamManager admin = pravegaContainer.getAdmin();
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "input-topic-2-partitions"
                                    creation-mode: create-if-not-exists
                                    deletion-mode: none
                                    partitions: 2
                                  - name: "input-topic-delete"
                                    creation-mode: create-if-not-exists
                                    deletion-mode: delete
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        try (ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .topicConnectionsRuntimeRegistry(new TopicConnectionsRuntimeRegistry())
                        .build()) {

            Module module = applicationInstance.getModule("module-1");

            ExecutionPlan implementation =
                    deployer.createImplementation("app", applicationInstance);
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("input-topic")))
                            instanceof PravegaTopic);

            deployer.setup("tenant", implementation);
            deployer.deploy("tenant", implementation, null);

            assertTrue(admin.checkStreamExists("langstream", "input-topic"));
            assertTrue(admin.checkStreamExists("langstream", "input-topic-2-partitions"));
            assertTrue(admin.checkStreamExists("langstream", "input-topic-delete"));

            deployer.delete("tenant", implementation, null);
            deployer.cleanup("tenant", implementation);

            assertFalse(admin.checkStreamExists("langstream", "input-topic-delete"));
            assertTrue(admin.checkStreamExists("langstream", "input-topic-2-partitions"));
            assertTrue(admin.checkStreamExists("langstream", "input-topic"));
        }
    }

    private static String buildInstanceYaml() {
        return """
                     instance:
                       streamingCluster:
                         type: "pravega"
                         configuration:
                           client:
                             controller-uri: "%s"
                             scope: "langstream"
                       computeCluster:
                         type: "none"
                     """
                .formatted(pravegaContainer.getControllerUri());
    }
}
