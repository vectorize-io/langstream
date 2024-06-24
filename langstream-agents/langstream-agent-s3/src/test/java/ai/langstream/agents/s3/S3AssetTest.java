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
package ai.langstream.agents.s3;

import ai.langstream.ai.agents.commons.state.S3StateStorage;
import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.*;
import io.minio.*;
import io.minio.messages.Item;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

@Testcontainers
public class S3AssetTest {
    private static final DockerImageName localstackImage =
            DockerImageName.parse("localstack/localstack:2.2.0");

    @Container
    private static final LocalStackContainer localstack =
            new LocalStackContainer(localstackImage).withServices(S3);

    @Test
    void testWithAlreadyDeletedBucket() throws Exception {

        AssetManager assetManager = new S3AssetsManagerProvider()
                .createInstance("s3-bucket");

        assetManager.initialize(new AssetDefinition("my-asset", "my-assert", "create-if-not-exists", "delete", "s3-bucket", Map.of(
                "bucket-name", "test",
                "endpoint", localstack.getEndpointOverride(S3).toString()
        )));
        assertFalse(assetManager.assetExists());
        assetManager.deployAsset();
        assertTrue(assetManager.assetExists());
        // must not fail
        assetManager.deployAsset();
        assertTrue(assetManager.assetExists());
        assetManager.deleteAssetIfExists();
        assertFalse(assetManager.assetExists());
        // must not fail
        assetManager.deleteAssetIfExists();
        assertFalse(assetManager.assetExists());
    }
}
