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
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PineconeAssetsProvider extends AbstractAssetProvider {

    public PineconeAssetsProvider() {
        super(Set.of("pinecone-index"));
    }

    @Override
    protected Class getAssetConfigModelClass(String type) {
        return TableConfig.class;
    }

    @Override
    protected boolean lookupResource(String fieldName) {
        return "datasource".equals(fieldName);
    }

    @AssetConfig(
            name = "Pinecone Serverless index",
            description =
                    """
                    Manage Pinecone Serverless index.
                    """)
    @Data
    public static class TableConfig {

        @ConfigProperty(
                description =
                        """
                       Reference to a datasource id configured in the application.
                       """,
                required = true)
        private String datasource;

        @ConfigProperty(
                description =
                        """
                       Name of the index. If not specified, the one defined in the datasource will be used.
                       """)
        private String name;

        @ConfigProperty(
                required = true,
                description =
                        """
                          Dimension of the vectors.
                       """)
        private int dimension;

        @ConfigProperty(
                required = true,
                description =
                        """
                            Metric used to compute the similarity between vectors.
                       """)
        private String metric;

        @ConfigProperty(
                required = true,
                description =
                        """
                            Cloud provider where the index is stored.
                       """)
        private String cloud;

        @ConfigProperty(
                required = true,
                description =
                        """
                            Region where the index is stored.
                       """)
        private String region;
    }
}
