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

import ai.langstream.agents.azureblobstorage.AzureBlobStorageSource;
import ai.langstream.agents.google.drive.GoogleDriveSource;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.testrunners.AbstractGenericStreamingApplicationRunner;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClient;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static ai.langstream.testrunners.AbstractApplicationRunner.INTEGRATION_TESTS_GROUP1;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

@Slf4j
@Testcontainers
@Tag(INTEGRATION_TESTS_GROUP1)
@Disabled
class GoogleDriveSourceIT extends AbstractGenericStreamingApplicationRunner {
    @Container
    private static final LocalStackContainer localstack =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.2.0"))
                    .withServices(S3);

    private static final String CREDENTIALS_JSON = """
             {
                }            
            """.replace("\n", "");


    @Test
    public void test() throws Exception {
        try (GoogleDriveSource.GDriveClient client = new GoogleDriveSource.GDriveClient(CREDENTIALS_JSON, DriveScopes.DRIVE);) {

            final String appId = "app-" + UUID.randomUUID().toString().substring(0, 4);

            String tenant = "tenant";

            final String folderName = UUID.randomUUID().toString();
            File folderMetadata = new File();
            folderMetadata.setName(folderName);
            folderMetadata.setMimeType("application/vnd.google-apps.folder");
            String folderId = client.getClient().files().create(folderMetadata).execute().getId();

            String[] expectedAgents = new String[]{appId + "-step1"};
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
                                      - type: "google-drive-source"
                                        id: "step1"
                                        output: "${globals.output-topic}"
                                        configuration:
                                            service-account-json: '%s'
                                            state-storage: s3
                                            state-storage-s3-bucket: "test-state-bucket"
                                            state-storage-s3-endpoint: "%s"
                                            deleted-objects-topic: "deleted-objects"
                                            idle-time: 1
                                            root-parents: [%s]
                                    """
                                    .formatted(CREDENTIALS_JSON, localstack.getEndpointOverride(S3), folderId));

            List<String> fileIds = new ArrayList<>();



            for (int i = 0; i < 2; i++) {
                final String s = "content" + i;
                File fileMetadata = new File();
                fileMetadata.setName("test-" + i + ".txt");
                fileMetadata.setParents(List.of(folderId));
                ByteArrayContent byteArrayContent = ByteArrayContent.fromString("text/plain", s);
                String id = client.getClient().files().create(fileMetadata, byteArrayContent)
                        .setFields("id")
                        .execute()
                        .getId();
                fileIds.add(id);
            }

            try (ApplicationRuntime applicationRuntime =
                         deployApplication(
                                 tenant, appId, application, buildInstanceYaml(), expectedAgents)) {

                try (TopicConsumer deletedDocumentsConsumer = createConsumer("deleted-objects");
                     TopicConsumer consumer =
                             createConsumer(applicationRuntime.getGlobal("output-topic"));) {

                    executeAgentRunners(applicationRuntime);
                    waitForMessages(
                            consumer,
                            2,
                            (consumerRecords, objects) -> {
                                assertEquals(2, consumerRecords.size());
                                assertEquals(fileIds.get(0), consumerRecords.get(0).key());
                                if (consumerRecords.get(0).value() instanceof String) {
                                    assertEquals("content0", consumerRecords.get(0).value());
                                } else {
                                    assertEquals(
                                            "content0",
                                            new String((byte[]) consumerRecords.get(0).value()));
                                }
                                assertRecordHeadersEquals(
                                        consumerRecords.get(0),
                                        Map.of(
                                                "bucket",
                                                "",
                                                "drive-filename",
                                                "test-0.txt",
                                                "content_diff",
                                                "new",
                                                "name",
                                                fileIds.get(0)));

                                assertEquals(fileIds.get(1), consumerRecords.get(1).key());
                                if (consumerRecords.get(1).value() instanceof String) {
                                    assertEquals("content1", consumerRecords.get(1).value());
                                } else {
                                    assertEquals(
                                            "content1",
                                            new String((byte[]) consumerRecords.get(1).value()));
                                }
                                assertRecordHeadersEquals(
                                        consumerRecords.get(1),
                                        Map.of(
                                                "bucket",
                                                "",
                                                "drive-filename",
                                                "test-1.txt",
                                                "content_diff",
                                                "new",
                                                "name",
                                                fileIds.get(1)));
                            });


                    client.getClient().files().delete(fileIds.get(0)).execute();
                    executeAgentRunners(applicationRuntime);

                    waitForMessages(deletedDocumentsConsumer, List.of(fileIds.get(0)));
                }
            }
        }
    }
}
