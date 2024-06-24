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

import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.assets.AssetManagerProvider;
import ai.langstream.api.util.ConfigurationUtils;
import io.minio.BucketExistsArgs;
import io.minio.MinioClient;
import io.minio.RemoveBucketArgs;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class S3AssetsManagerProvider implements AssetManagerProvider {

    @Override
    public boolean supports(String assetType) {
        return "s3-bucket".equals(assetType);
    }

    @Override
    public AssetManager createInstance(String assetType) {

        switch (assetType) {
            case "s3-bucket":
                return new S3BucketAssetManager();
            default:
                throw new IllegalArgumentException();
        }
    }

    private static class S3BucketAssetManager implements AssetManager {

        MinioClient client;

        String bucketName;

        @Override
        public void initialize(AssetDefinition assetDefinition) throws Exception {
            Map<String, Object> configuration = assetDefinition.getConfig();
            bucketName = ConfigurationUtils.requiredField(configuration, "bucket-name", () -> "s3-bucket asset");
            String endpoint = ConfigurationUtils.requiredField(configuration, "endpoint", () -> "s3-bucket asset");
            String accessKey = ConfigurationUtils.getString("access-key", null, configuration);
            String secretKey = ConfigurationUtils.getString("secret-key", null, configuration);
            String region = ConfigurationUtils.getString("region", null, configuration);
            MinioClient.Builder builder =
                    MinioClient.builder()
                            .endpoint(endpoint);
            if (accessKey != null) {
                builder.credentials(accessKey, secretKey);
            }
            if (region != null) {
                builder.region(region);
            }
            client = builder.build();
        }

        @Override
        public boolean assetExists() throws Exception {
            return client.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
        }

        @Override
        public void deployAsset() throws Exception {
            S3Utils.makeBucketIfNotExists(client, bucketName);
        }

        @Override
        public boolean deleteAssetIfExists() throws Exception {
            if (assetExists()) {
                log.info("Deleting bucket {}", bucketName);
                client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
                return true;
            } else {
                log.info("Bucket {} does not exist", bucketName);
            }
            return false;
        }

        @Override
        public void close() throws Exception {
            if (client != null) {
                client.close();
            }
        }
    }
}
