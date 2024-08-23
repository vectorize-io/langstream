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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.testrunners.AbstractGenericStreamingApplicationRunner;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.DeleteErrorException;
import com.dropbox.core.v2.files.FileMetadata;
import com.microsoft.graph.models.*;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
@Tag(INTEGRATION_TESTS_GROUP1)
@Disabled
class DropboxSourceIT extends AbstractGenericStreamingApplicationRunner {
    @Container
    private static final LocalStackContainer localstack =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.2.0"))
                    .withServices(S3);

    private static final String ACCESS_TOKEN = "";

    @Test
    public void test() throws Exception {

        DbxRequestConfig config =
                DbxRequestConfig.newBuilder("langstream-test").withAutoRetryEnabled().build();
        DbxClientV2 client = new DbxClientV2(config, ACCESS_TOKEN);
        try {
            client.files().deleteV2("/langstream-test");
        } catch (DeleteErrorException e) {
        }
        client.files().createFolderV2("/langstream-test");

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
                                  - type: "dropbox-source"
                                    id: "step1"
                                    configuration:
                                        access-token: %s
                                        path-prefix: /langstream-test
                                        state-storage: s3
                                        state-storage-s3-bucket: "test-state-bucket"
                                        state-storage-s3-endpoint: "%s"
                                        deleted-objects-topic: "deleted-objects"
                                        idle-time: 1
                                  - type: text-extractor
                                    id: step2
                                    output: "${globals.output-topic}"
                                """
                                .formatted(ACCESS_TOKEN, localstack.getEndpointOverride(S3)));

        List<String> fileIds = new ArrayList<>();
        List<byte[]> docs =
                List.of(
                        DropboxSourceIT.class.getResourceAsStream("/doc1.docx").readAllBytes(),
                        DropboxSourceIT.class.getResourceAsStream("/doc2.docx").readAllBytes());

        for (int i = 0; i < 2; i++) {
            byte[] bytes = docs.get(i);

            FileMetadata fileMetadata =
                    client.files()
                            .upload("/langstream-test/test-" + i + ".docx")
                            .uploadAndFinish(new ByteArrayInputStream(bytes));

            fileIds.add(fileMetadata.getId());
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
                        2,
                        (consumerRecords, objects) -> {
                            assertEquals(2, consumerRecords.size());
                            assertEquals(fileIds.get(0), consumerRecords.get(0).key());
                            assertTrue(
                                    ((String) consumerRecords.get(0).value())
                                            .contains("This is a document"));
                            assertEquals(fileIds.get(1), consumerRecords.get(1).key());
                            assertTrue(
                                    ((String) consumerRecords.get(1).value())
                                            .contains("This is a another document"));
                        });

                client.files().deleteV2("/langstream-test/test-0.docx");
                executeAgentRunners(applicationRuntime);
                waitForMessages(deletedDocumentsConsumer, List.of(fileIds.get(0)));
            }
        }
    }
}
