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
package ai.langstream.agents.vector.couchbase;

import ai.langstream.agents.vector.InterpolationUtils;
import ai.langstream.ai.agents.commons.jstl.JstlFunctions;
import ai.langstream.ai.agents.datasource.DataSourceProvider;
import ai.langstream.api.util.ObjectMapperFactory;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.SearchRequest;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.search.vector.VectorQuery;
import com.couchbase.client.java.search.vector.VectorSearch;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CouchbaseDataSource implements DataSourceProvider {

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "couchbase".equals(dataSourceConfig.get("service"));
    }

    @Data
    public static final class CouchbaseConfig {
        @JsonProperty(value = "connection-string", required = true)
        private String connectionString;

        @JsonProperty(value = "username", required = true)
        private String username;

        @JsonProperty(value = "password", required = true)
        private String password;
    }

    @Override
    public QueryStepDataSource createDataSourceImplementation(
            Map<String, Object> dataSourceConfig) {
        CouchbaseConfig config =
                ObjectMapperFactory.getDefaultMapper()
                        .convertValue(dataSourceConfig, CouchbaseConfig.class);
        return new CouchbaseQueryStepDataSource(config);
    }

    public static class CouchbaseQueryStepDataSource implements QueryStepDataSource {

        @Getter private final CouchbaseConfig clientConfig;
        private Cluster cluster;

        public CouchbaseQueryStepDataSource(CouchbaseConfig clientConfig) {
            this.clientConfig = clientConfig;
        }

        @Override
        public void initialize(Map<String, Object> config) {
            cluster =
                    Cluster.connect(
                            clientConfig.connectionString,
                            clientConfig.username,
                            clientConfig.password);
            log.info("Connected to Couchbase: {}", clientConfig.connectionString);
        }

        @Override
        public List<Map<String, Object>> fetchData(String query, List<Object> params) {
            try {
                Map<String, Object> queryMap =
                        InterpolationUtils.buildObjectFromJson(query, Map.class, params);
                if (queryMap.isEmpty()) {
                    throw new UnsupportedOperationException("Query is empty");
                }

                List<?> vectorLog = (List<?>) queryMap.get("vector");
                List<?> subList = vectorLog.size() > 5 ? vectorLog.subList(0, 5) : vectorLog;
                float[] vector = JstlFunctions.toArrayOfFloat(queryMap.remove("vector"));
                log.info("Query: {} {}", subList, queryMap);
                Integer topK = (Integer) queryMap.remove("topK");
                String bucketName = (String) queryMap.remove("bucket-name");
                String scopeName = (String) queryMap.remove("scope-name");
                String collectionName = (String) queryMap.remove("collection-name");
                String vectorIndexName = (String) queryMap.remove("index-name");
                Map<String, Object> filter = (Map<String, Object>) queryMap.get("filter");
                SearchRequest vectorSearchRequest;
                // if the values in the filter are empty then remove them from the map
                log.info("Filter: {}", filter);
                if (filter != null) {
                    filter.entrySet()
                            .removeIf(
                                    entry ->
                                            entry.getValue() == null
                                                    || entry.getValue().toString().isEmpty());
                }
                // if (filter == null || filter.isEmpty()) {
                //     queryMap.remove("filter");
                // }
                log.info("filter after removing empty values: {}", filter);
                if (queryMap.containsKey("filter") && filter != null && !filter.isEmpty()) {
                    List<SearchQuery> filterQueries = new ArrayList<>();

                    for (Map.Entry<String, Object> entry : filter.entrySet()) {
                        String filterField = entry.getKey();
                        String filterValue = entry.getValue().toString();
                        filterQueries.add(SearchQuery.match(filterValue).field(filterField));
                    }

                    // Combine all filter queries into a conjunctive query
                    SearchQuery searchQuery =
                            SearchQuery.conjuncts(filterQueries.toArray(new SearchQuery[0]));
                    // print search query
                    log.info("Search query: {}", searchQuery);

                    // Perform the vector search on the filtered documents
                    vectorSearchRequest =
                            SearchRequest.create(searchQuery)
                                    .vectorSearch(
                                            VectorSearch.create(
                                                    VectorQuery.create("vector", vector)
                                                            .numCandidates(topK)));
                } else {
                    // Perform the vector search without any filter
                    vectorSearchRequest =
                            SearchRequest.create(
                                    VectorSearch.create(
                                            VectorQuery.create("vector", vector)
                                                    .numCandidates(topK)));
                }
                SearchResult vectorSearchResult =
                        cluster.search(
                                bucketName + "." + scopeName + "." + vectorIndexName,
                                vectorSearchRequest);

                // Process and collect results
                List<Map<String, Object>> results =
                        vectorSearchResult.rows().stream()
                                .limit(topK)
                                .map(
                                        hit -> {
                                            final Map<String, Object> result = new HashMap<>();

                                            // Fetch and add the document content
                                            try {
                                                String documentId = hit.id();
                                                GetResult getResult =
                                                        cluster.bucket(bucketName)
                                                                .scope(scopeName)
                                                                .collection(collectionName)
                                                                .get(documentId);

                                                if (getResult != null) {
                                                    JsonObject content =
                                                            getResult.contentAsObject();

                                                    // Ensure the embeddings array exists
                                                    JsonArray embeddingsArray =
                                                            content.getArray("vector");
                                                    if (embeddingsArray != null) {
                                                        double[] embeddings =
                                                                new double[embeddingsArray.size()];
                                                        for (int i = 0;
                                                                i < embeddingsArray.size();
                                                                i++) {
                                                            embeddings[i] =
                                                                    embeddingsArray.getDouble(i);
                                                        }
                                                        // remove the embeddings array from the
                                                        // output
                                                        content.removeKey("vector");
                                                        // Ensure all filter fields match their
                                                        // corresponding filter values
                                                        if (filter != null && !filter.isEmpty()) {
                                                            // Ensure all filter fields match their
                                                            // corresponding filter values

                                                            boolean filtersMatch = true;
                                                            for (Map.Entry<String, Object> entry :
                                                                    filter.entrySet()) {
                                                                String field = entry.getKey();
                                                                String value =
                                                                        (String) entry.getValue();
                                                                log.info("Filter field: {}", field);
                                                                log.info("Filter value: {}", value);
                                                                log.info(
                                                                        "(filter) content value {}",
                                                                        content.getString(field));
                                                                // Ensure the filter field exists in
                                                                // the document and isn't ""
                                                                if (content.containsKey(field)
                                                                        && !content.getString(field)
                                                                                .isEmpty()
                                                                        && !content.getString(field)
                                                                                .equals(value)) {
                                                                    filtersMatch = false;
                                                                    log.info(
                                                                            "Document {} has {} {} instead of {}",
                                                                            documentId,
                                                                            field,
                                                                            content.getString(
                                                                                    field),
                                                                            value);
                                                                    break;
                                                                }
                                                            }

                                                            if (filtersMatch) {
                                                                result.put("id", hit.id());

                                                                // Calculate and add cosine
                                                                // similarity
                                                                double cosineSimilarity =
                                                                        computeCosineSimilarity(
                                                                                vector, embeddings);
                                                                result.put(
                                                                        "similarity",
                                                                        cosineSimilarity);
                                                                result.putAll(content.toMap());
                                                            }
                                                        } else {
                                                            // If there are no filters, process the
                                                            // result directly
                                                            result.put("id", hit.id());
                                                            // Calculate and add cosine similarity
                                                            double cosineSimilarity =
                                                                    computeCosineSimilarity(
                                                                            vector, embeddings);
                                                            result.put(
                                                                    "similarity", cosineSimilarity);
                                                            result.putAll(content.toMap());
                                                        }
                                                    }
                                                }
                                            } catch (DocumentNotFoundException e) {
                                                log.error(
                                                        "Document not found for ID: {}",
                                                        hit.id(),
                                                        e);
                                            } catch (Exception e) {
                                                log.error(
                                                        "Error retrieving document content for ID: {}",
                                                        hit.id(),
                                                        e);
                                            }

                                            return result;
                                        })
                                .filter(result -> !result.isEmpty())
                                .sorted(
                                        (r1, r2) ->
                                                Double.compare(
                                                        (Double) r2.get("similarity"),
                                                        (Double) r1.get("similarity")))
                                .collect(Collectors.toList());

                return results;

            } catch (Exception e) {
                log.error("Error executing query: {}", e.getMessage(), e);
                throw new RuntimeException("Error during search", e);
            }
        }

        private double computeCosineSimilarity(float[] vector1, double[] vector2) {
            // Log the first 5 elements of each vector and the operation
            log.debug(
                    "Vector1 (first 5 elements): {}..., Vector2 (first 5 elements): {}..., Computing cosine similarity between vectors",
                    Arrays.toString(Arrays.copyOfRange(vector1, 0, 5)),
                    Arrays.toString(Arrays.copyOfRange(vector2, 0, 5)));
            double dotProduct = 0.0;
            double normA = 0.0;
            double normB = 0.0;
            for (int i = 0; i < vector1.length; i++) {
                dotProduct += vector1[i] * vector2[i];
                normA += Math.pow(vector1[i], 2);
                normB += Math.pow(vector2[i], 2);
            }
            return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
        }

        @Override
        public void close() {
            if (cluster != null) {
                cluster.disconnect();
                log.info("Disconnected from Couchbase Bucket: {}", clientConfig.connectionString);
            }
        }
    }
}
