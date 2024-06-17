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
package ai.langstream.utils;

import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class HerdDBExtension implements BeforeAllCallback, AfterAllCallback {
    public static final DockerImageName DOCKER_IMAGE_NAME =
            DockerImageName.parse("herddb/herddb:0.28.0");
    private GenericContainer container;

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {

        if (container != null) {
            container.close();
        }
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        container =
                new GenericContainer<>(DOCKER_IMAGE_NAME)
                        .withExposedPorts(7000)
                        .withLogConsumer(
                                new Consumer<OutputFrame>() {
                                    @Override
                                    public void accept(OutputFrame outputFrame) {
                                        log.debug("herddb> {}", outputFrame.getUtf8String().trim());
                                    }
                                });
        ;
        container.start();
    }

    public String getJDBCUrl() {
        return "jdbc:herddb:server:localhost:" + container.getMappedPort(7000);
    }
}
