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
package ai.langstream.agents.vector.pinecone;

import ai.langstream.agents.vector.opensearch.OpenSearchDataSource;
import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.assets.AssetManagerProvider;
import ai.langstream.api.util.ConfigurationUtils;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pinecone.exceptions.PineconeNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.openapitools.client.model.IndexModel;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.indices.*;
import org.opensearch.client.util.MissingRequiredPropertyException;

import java.util.Map;

@Slf4j
public class PineconeAssetsManagerProvider implements AssetManagerProvider {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public boolean supports(String assetType) {
        return "pinecone-index".equals(assetType);
    }

    @Override
    public AssetManager createInstance(String assetType) {

        switch (assetType) {
            case "pinecone-index":
                return new PineconeIndexAssetManager();
            default:
                throw new IllegalArgumentException();
        }
    }



    public record PineconeIndexConfig(
            String name,
            int dimension,
            String metric,
            String cloud,
            String region
    ) {

    }

    private static class PineconeIndexAssetManager implements AssetManager {


        PineconeDataSource.PineconeQueryStepDataSource datasource;


        private PineconeIndexConfig indexConfig;

        @Override
        public void initialize(AssetDefinition assetDefinition) throws Exception {
            this.datasource = buildDataSource(assetDefinition);
            this.indexConfig = MAPPER.convertValue(assetDefinition.getConfig(), PineconeIndexConfig.class);
        }

        @Override
        public boolean assetExists() throws Exception {
            String indexName = getIndexName();
            try {
                datasource
                        .getClient()
                        .describeIndex(indexName);
                return true;
            } catch (PineconeNotFoundException e) {
                return false;
            }
        }

        private String getIndexName() {
            return indexConfig.name() != null ? indexConfig.name(): datasource.getClientConfig().getIndexName();
        }

        @Override
        public void deployAsset() throws Exception {
            String name = getIndexName();
            log.info("Creating index {}", name);
            datasource.getClient()
                    .createServerlessIndex(
                            name,
                            indexConfig.metric(),
                            indexConfig.dimension(),
                            indexConfig.cloud(),
                            indexConfig.region()
                    );
        }

        @Override
        public boolean deleteAssetIfExists() throws Exception {
            String name = getIndexName();
            log.info("Deleting index {}", name);
            try {
                datasource
                        .getClient()
                        .deleteIndex(name);
                return true;
            } catch (PineconeNotFoundException e) {
                return false;
            }
        }

        @Override
        public void close() throws Exception {
            if (datasource != null) {
                datasource.close();
            }
        }
    }

    private static PineconeDataSource.PineconeQueryStepDataSource buildDataSource(
            AssetDefinition assetDefinition) {
        PineconeDataSource dataSource = new PineconeDataSource();
        Map<String, Object> datasourceDefinition =
                ConfigurationUtils.getMap("datasource", Map.of(), assetDefinition.getConfig());
        Map<String, Object> configuration =
                ConfigurationUtils.getMap("configuration", Map.of(), datasourceDefinition);
        PineconeDataSource.PineconeQueryStepDataSource result =
                dataSource.createDataSourceImplementation(configuration);
        result.initialize(null);
        return result;
    }
}
