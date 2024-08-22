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
package ai.langstream.agents.vector.datasource.impl;

import static junit.framework.TestCase.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.langstream.agents.vector.VectorDBSinkAgent;
import ai.langstream.agents.vector.pinecone.PineconeAssetsManagerProvider;
import ai.langstream.agents.vector.pinecone.PineconeDataSource;
import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.MetricsReporter;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.util.ObjectMapperFactory;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Slf4j
@Disabled
class PineconeDataSourceTest {

    static final String API_KEY = "xx";
    static final String INDEX_NAME = "test";

    @Test
    void testPineconeWrite() throws Exception {

        Map<String, Object> datasourceConfig =
                Map.of("service", "pinecone", "api-key", API_KEY, "index-name", INDEX_NAME);

        List<List<Float>> vectors = new ArrayList<>();
        for (int k = 0; k < 3; k++) {
            List<Float> vector = new ArrayList<>();
            for (int i = 1; i <= 1536; i++) {
                vector.add(1f / i * (k + 1));
            }
            vectors.add(vector);
        }

        try (VectorDBSinkAgent agent =
                (VectorDBSinkAgent)
                        new AgentCodeRegistry().getAgentCode("vector-db-sink").agentCode(); ) {
            Map<String, Object> configuration = new HashMap<>();
            configuration.put("datasource", datasourceConfig);

            configuration.put("vector.id", "value.id");
            configuration.put("vector.vector", "value.vector");
            configuration.put("vector.namespace", "");
            configuration.put("vector.metadata.genre", "value.genre");
            configuration.put("vector.metadata.artist", "value.artist");
            configuration.put("vector.metadata.title", "value.title");

            AgentContext agentContext = mock(AgentContext.class);
            when(agentContext.getMetricsReporter()).thenReturn(MetricsReporter.DISABLED);
            agent.init(configuration);
            agent.start();
            agent.setContext(agentContext);

            for (int i = 0; i < 3; i++) {
                String genre = "genre-" + i;
                String title = "title-" + i;

                Map<String, Object> value =
                        Map.of("id", i, "vector", vectors.get(i), "genre", genre, "title", title);
                SimpleRecord record =
                        SimpleRecord.of(
                                null,
                                ObjectMapperFactory.getDefaultMapper().writeValueAsString(value));
                List<Record> committed = new CopyOnWriteArrayList<>();
                agent.write(record).thenRun(() -> committed.add(record)).get();
                assertEquals(committed.get(0), record);
            }
        }
        // A 20s sleep is needed on the legacy "pod" databases. The serverless databases
        // work with a 5s sleep.
        // Sleep for a while to allow the data to be indexed
        log.info("Sleeping for 5 seconds to allow the data to be indexed");
        Thread.sleep(5000);

        PineconeDataSource dataSource = new PineconeDataSource();
        try (QueryStepDataSource implementation =
                dataSource.createDataSourceImplementation(datasourceConfig); ) {

            implementation.initialize(null);

            String query =
                    """
                            {
                                  "vector": ?,
                                  "topK": 5,
                                  "filter": {
                                      "genre": {"$eq": ?}
                                  }
                                }
                            """;
            List<Object> params = new ArrayList<>(Arrays.asList(vectors.get(0), "genre-0"));
            List<Map<String, Object>> results = implementation.fetchData(query, params);
            log.info("Results: {}", results);

            assertEquals(1, results.size());

            // Query with genre resolved to null (which removes the filter)
            params = new ArrayList<>(Arrays.asList(vectors.get(0), null));
            results = implementation.fetchData(query, params);
            log.info("Results: {}", results);

            assertEquals(3, results.size());

            // Query with genre and title
            query =
                    """
                            {
                                  "vector": ?,
                                  "topK": 5,
                                  "filter": {
                                      "genre": {"$eq": ?},
                                      "title": {"$eq": ?}
                                  }
                                }
                            """;

            params = new ArrayList<>(Arrays.asList(vectors.get(0), "genre-2", "title-2"));
            results = implementation.fetchData(query, params);
            log.info("Results: {}", results);

            assertEquals(1, results.size());

            // Query with genre resolved to null (which removes the filter)

            params = new ArrayList<>(Arrays.asList(vectors.get(0), null, "title-2"));
            results = implementation.fetchData(query, params);
            log.info("Results: {}", results);

            assertEquals(1, results.size());

            // Query with genre and title resolved to null (which removes the filters)

            params = new ArrayList<>(Arrays.asList(vectors.get(0), null, null));
            results = implementation.fetchData(query, params);
            log.info("Results: {}", results);

            assertEquals(3, results.size());
        }
    }

    @Test
    void testPineconeQuery() throws Exception {
        PineconeDataSource dataSource = new PineconeDataSource();
        Map<String, Object> config = Map.of("api-key", API_KEY, "index-name", INDEX_NAME);
        QueryStepDataSource implementation = dataSource.createDataSourceImplementation(config);
        implementation.initialize(null);

        String query =
                """
                {
                      "vector": ?,
                      "topK": 5
                    }
                """;
        List<Object> params = List.of(List.of(0.1f, 0.1f, 0.1f, 0.1f, 0.1f));
        List<Map<String, Object>> results = implementation.fetchData(query, params);
        log.info("Results: {}", results);
    }

    @Test
    void testPineconeQueryWithFilter() throws Exception {
        PineconeDataSource dataSource = new PineconeDataSource();
        Map<String, Object> config =
                Map.of(
                        "api-key", API_KEY,
                        "index-name", INDEX_NAME);
        QueryStepDataSource implementation = dataSource.createDataSourceImplementation(config);
        implementation.initialize(null);

        String query =
                """
                {
                      "vector": ?,
                      "topK": 5,
                      "filter":
                        {"$and": [{"genre": ?}, {"year":?}]}
                    }
                """;
        List<Object> params = List.of(List.of(0.1f, 0.1f, 0.1f, 0.1f, 0.1f), "comedy", 2019);
        List<Map<String, Object>> results = implementation.fetchData(query, params);
        log.info("Results: {}", results);
    }

    @Test
    void testAsset() throws Exception {
        final Map<String, Object> assetConfig =
                Map.of(
                        "cloud",
                        "aws",
                        "region",
                        "us-east-1",
                        "metric",
                        "cosine",
                        "dimension",
                        1536,
                        "datasource",
                        Map.of(
                                "configuration",
                                Map.of("api-key", API_KEY, "index-name", INDEX_NAME)));
        final AssetManager instance = createAssetManager(assetConfig);

        instance.deleteAssetIfExists();
        instance.deleteAssetIfExists();
        assertFalse(instance.assetExists());
        instance.deployAsset();
        assertTrue(instance.assetExists());
        instance.deleteAssetIfExists();
    }

    private AssetManager createAssetManager(Map<String, Object> config) throws Exception {
        final AssetManager instance =
                new PineconeAssetsManagerProvider().createInstance("pinecone-index");
        final AssetDefinition def = new AssetDefinition();
        def.setConfig(config);
        instance.initialize(def);
        return instance;
    }
}
