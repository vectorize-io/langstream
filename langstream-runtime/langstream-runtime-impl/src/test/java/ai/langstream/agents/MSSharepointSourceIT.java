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
import static org.junit.jupiter.api.Assertions.*;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.testrunners.AbstractGenericStreamingApplicationRunner;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.microsoft.graph.models.*;
import com.microsoft.graph.serviceclient.GraphServiceClient;
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
class MSSharepointSourceIT extends AbstractGenericStreamingApplicationRunner {
    @Container
    private static final LocalStackContainer localstack =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.2.0"))
                    .withServices(S3);

    private static final String TENANT_ID = "";
    private static final String CLIENT_ID = "";
    private static final String CLIENT_SECRET = "";

    private static final String SITE_ID = "sito2";

    @Test
    public void test() throws Exception {
        GraphServiceClient client = newClient();

        final String appId = "app-" + UUID.randomUUID().toString().substring(0, 4);

        String tenant = "tenant";
        List<Site> sites = client.sites().getAllSites().get().getValue();
        Site selectedSite =
                sites.stream()
                        .filter(
                                site ->
                                        SITE_ID.equals(site.getId())
                                                || SITE_ID.equals(site.getName()))
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("Site not found"));

        Drive drive = client.sites().bySiteId(selectedSite.getId()).drive().get();
        DriveItem rootItem = client.drives().byDriveId(drive.getId()).root().get();

        DriveItem folderItem = new DriveItem();
        folderItem.setName("LangStream test");
        folderItem.setFolder(new Folder());
        List<DriveItem> children =
                client.drives()
                        .byDriveId(drive.getId())
                        .items()
                        .byDriveItemId(rootItem.getId())
                        .children()
                        .get()
                        .getValue();
        for (DriveItem child : children) {
            if (child.getName().equals(folderItem.getName())) {
                client.drives()
                        .byDriveId(drive.getId())
                        .items()
                        .byDriveItemId(child.getId())
                        .delete();
            }
        }
        String folderId =
                client.drives()
                        .byDriveId(drive.getId())
                        .items()
                        .byDriveItemId(rootItem.getId())
                        .children()
                        .post(folderItem)
                        .getId();

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
                                  - type: "ms365-sharepoint-source"
                                    id: "step1"
                                    configuration:
                                        ms-client-id: %s
                                        ms-client-secret: %s
                                        ms-tenant-id: %s
                                        sites: [%s]
                                        state-storage: s3
                                        state-storage-s3-bucket: "test-state-bucket"
                                        state-storage-s3-endpoint: "%s"
                                        deleted-objects-topic: "deleted-objects"
                                        idle-time: 1
                                  - type: text-extractor
                                    id: step2
                                    output: "${globals.output-topic}"
                                """
                                .formatted(
                                        CLIENT_ID,
                                        CLIENT_SECRET,
                                        TENANT_ID,
                                        SITE_ID,
                                        localstack.getEndpointOverride(S3)));

        List<String> fileIds = new ArrayList<>();
        List<String> itemIds = new ArrayList<>();
        List<byte[]> docs =
                List.of(
                        MSSharepointSourceIT.class.getResourceAsStream("/doc1.docx").readAllBytes(),
                        MSSharepointSourceIT.class
                                .getResourceAsStream("/doc2.docx")
                                .readAllBytes());

        for (int i = 0; i < 2; i++) {
            byte[] bytes = docs.get(i);
            DriveItem driveItem = new DriveItem();
            driveItem.setName("test-" + i + ".docx");
            driveItem.setFile(new File());
            DriveItem item =
                    client.drives()
                            .byDriveId(drive.getId())
                            .items()
                            .byDriveItemId(folderId)
                            .children()
                            .post(driveItem);

            client.drives()
                    .byDriveId(drive.getId())
                    .items()
                    .byDriveItemId(item.getId())
                    .content()
                    .put(new ByteArrayInputStream(bytes));
            fileIds.add(selectedSite.getId() + "_" + item.getId());
            itemIds.add(item.getId());
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

                client.drives()
                        .byDriveId(drive.getId())
                        .items()
                        .byDriveItemId(itemIds.get(0))
                        .delete();
                executeAgentRunners(applicationRuntime);
                waitForMessages(deletedDocumentsConsumer, List.of(fileIds.get(0)));
            }
        }
    }

    private static GraphServiceClient newClient() {
        final String[] scopes = new String[] {"https://graph.microsoft.com/.default"};
        final ClientSecretCredential credential =
                new ClientSecretCredentialBuilder()
                        .clientId(CLIENT_ID)
                        .tenantId(TENANT_ID)
                        .clientSecret(CLIENT_SECRET)
                        .build();

        return new GraphServiceClient(credential, scopes);
    }
}
