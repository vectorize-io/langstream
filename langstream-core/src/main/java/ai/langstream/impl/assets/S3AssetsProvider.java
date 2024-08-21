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
package ai.langstream.impl.assets;

import ai.langstream.api.doc.AssetConfig;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.impl.common.AbstractAssetProvider;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class S3AssetsProvider extends AbstractAssetProvider {

    public S3AssetsProvider() {
        super(Set.of("s3-bucket"));
    }

    @Override
    protected Class getAssetConfigModelClass(String type) {
        return TableConfig.class;
    }

    @Override
    protected boolean lookupResource(String fieldName) {
        return false;
    }

    @AssetConfig(
            name = "S3 bucket",
            description =
                    """
                    Manage S3 bucket lifecycle.
                    """)
    @Data
    public static class TableConfig {

        @ConfigProperty(
                description =
                        """
                        The name of the bucket to read from.
                        """,
                required = true)
        @JsonProperty("bucket-name")
        private String bucketName;

        @ConfigProperty(
                description =
                        """
                        The endpoint of the S3 server.
                        """,
                required = true)
        private String endpoint;

        @ConfigProperty(
                description =
                        """
                        Access key for the S3 server.
                        """,
                required = false)
        @JsonProperty("access-key")
        private String accessKey;

        @ConfigProperty(
                description =
                        """
                        Secret key for the S3 server.
                        """,
                required = false)
        @JsonProperty("secret-key")
        private String secretKey;
    }
}
