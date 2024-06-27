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
package ai.langstream.kafka;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import io.minio.*;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
class S3AssetIT extends AbstractKafkaApplicationRunner {

    @Container
    private static final LocalStackContainer localstack =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.2.0"))
                    .withServices(S3);

    @Test
    public void test() throws Exception {

        final String appId = "app-" + UUID.randomUUID().toString().substring(0, 4);

        String tenant = "tenant";

        String[] expectedAgents = new String[] {appId + "-step1"};
        String endpoint = localstack.getEndpointOverride(S3).toString();
        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                assets:
                                  - name: "my-bucket"
                                    asset-type: "s3-bucket"
                                    creation-mode: create-if-not-exists
                                    deletion-mode: delete
                                    config:
                                        bucket-name: "test-state-bucket"
                                        endpoint: "%s"
                                topics:
                                  - name: "${globals.output-topic}"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - type: "s3-source"
                                    id: "step1"
                                    output: "${globals.output-topic}"
                                    configuration:\s
                                        bucketName: "test-bucket"
                                        endpoint: "%s"
                                        state-storage: s3
                                        state-storage-s3-bucket: "test-state-bucket"
                                        state-storage-s3-endpoint: "%s"
                                        delete-objects: false
                                        idle-time: 1
                                """
                                .formatted(endpoint, endpoint, endpoint));

        MinioClient minioClient = MinioClient.builder().endpoint(endpoint).build();
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, appId, application, buildInstanceYaml(), expectedAgents)) {

            try (KafkaConsumer<String, String> deletedDocumentsConsumer =
                            createConsumer("deleted-objects");
                    KafkaConsumer<String, String> consumer =
                            createConsumer(applicationRuntime.getGlobal("output-topic")); ) {

                executeAgentRunners(applicationRuntime);
                assertTrue(
                        minioClient.bucketExists(
                                BucketExistsArgs.builder().bucket("test-bucket").build()));
                assertTrue(
                        minioClient.bucketExists(
                                BucketExistsArgs.builder().bucket("test-state-bucket").build()));
            }
        }
        assertTrue(
                minioClient.bucketExists(BucketExistsArgs.builder().bucket("test-bucket").build()));
        assertFalse(
                minioClient.bucketExists(
                        BucketExistsArgs.builder().bucket("test-state-bucket").build()));
    }
}
