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

import ai.langstream.agents.atlassian.confluence.client.ConfluenceRestAPIClient;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.testrunners.AbstractGenericStreamingApplicationRunner;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.DeleteErrorException;
import com.dropbox.core.v2.files.FileMetadata;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static ai.langstream.testrunners.AbstractApplicationRunner.INTEGRATION_TESTS_GROUP1;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

@Slf4j
@Testcontainers
@Tag(INTEGRATION_TESTS_GROUP1)
@Disabled
class ConfluenceSourceIT extends AbstractGenericStreamingApplicationRunner {
    @Container
    private static final LocalStackContainer localstack =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.2.0"))
                    .withServices(S3);

    private static final String USERNAME = "";
    private static final String API_TOKEN = "";
    private static final String DOMAIN = "";
    private static final String SPACE = "";

    @Test
    public void test() throws Exception {
        ConfluenceRestAPIClient confluence = new ConfluenceRestAPIClient(USERNAME, API_TOKEN, DOMAIN);

        long spaceId = confluence.findSpaceByNameOrKeyOrId(SPACE).iterator().next().id();
        confluence.visitSpacePages(spaceId, Set.of(), page -> {
            if (page.title().equals("Langstream Parent")) {
                confluence.deletePage(page.id());
            }
        });
        String parentPageId = confluence.createPage(spaceId, "Langstream Parent", "Parent page", null);


        final String appId = "app-" + UUID.randomUUID().toString().substring(0, 4);

        String tenant = "tenant";
        String[] expectedAgents = new String[] {appId + "-step1", appId + "-step2"};
        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "${globals.output-topic}"
                                    creation-mode: create-if-not-exists
                                  - name: "deleted-documents"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - type: "confluence-source"
                                    id: "step1"
                                    configuration:
                                        username: %s
                                        api-token: %s
                                        domain: %s
                                        spaces: [%s]
                                        state-storage: s3
                                        state-storage-s3-bucket: "test-state-bucket"
                                        state-storage-s3-endpoint: "%s"
                                        deleted-objects-topic: "deleted-objects"
                                        idle-time: 1
                                        root-parents: ["%s"]
                                  - type: text-extractor
                                    id: step2
                                    output: "${globals.output-topic}"
                                """
                                .formatted(USERNAME, API_TOKEN, DOMAIN, SPACE, localstack.getEndpointOverride(S3), parentPageId));

        List<String> pageIds = new ArrayList<>();
        pageIds.add(parentPageId);
        for (int i = 0; i < 2; i++) {
            String title = "Langstream test-" + i;
            confluence.deletePageByTitle(spaceId, title);
            String pageId = confluence.createPage(spaceId, title, "document " + i, parentPageId);
            pageIds.add(pageId);
        }

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, appId, application, buildInstanceYaml(), expectedAgents)) {

            try (TopicConsumer deletedDocumentsConsumer = createConsumer("deleted-objects");
                    TopicConsumer consumer =
                            createConsumer(applicationRuntime.getGlobal("output-topic")); ) {

                executeAgentRunners(applicationRuntime, 5);
                waitForMessages(
                        consumer,
                        3,
                        (consumerRecords, objects) -> {
                            assertEquals(3, consumerRecords.size());
                            assertEquals(pageIds.get(0), consumerRecords.get(0).key());
                            assertTrue(
                                    ((String) consumerRecords.get(0).value())
                                            .contains("Parent page"));
                            assertEquals(pageIds.get(1), consumerRecords.get(1).key());
                            assertTrue(
                                    ((String) consumerRecords.get(1).value())
                                            .contains("document 0"));
                            assertEquals(pageIds.get(2), consumerRecords.get(2).key());
                            assertTrue(
                                    ((String) consumerRecords.get(2).value())
                                            .contains("document 1"));
                        });

                confluence.deletePage(pageIds.get(1));
                executeAgentRunners(applicationRuntime, 5);
                waitForMessages(deletedDocumentsConsumer, List.of(pageIds.get(1)));
            }
        } finally {
            for (String pageId : pageIds) {
                try {
                    confluence.deletePage(pageId);
                } catch (Exception e) {
                    log.error("Error deleting page {}", pageId, e);
                }
            }
        }
    }
}
