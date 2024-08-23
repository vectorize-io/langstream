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

import static ai.langstream.agents.vector.InterpolationUtils.buildObjectFromJson;

import ai.langstream.ai.agents.datasource.DataSourceProvider;
import ai.langstream.api.util.ObjectMapperFactory;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.grpc.StatusRuntimeException;
import io.pinecone.clients.Index;
import io.pinecone.clients.Pinecone;
import io.pinecone.shadow.com.google.protobuf.ListValue;
import io.pinecone.shadow.com.google.protobuf.Struct;
import io.pinecone.shadow.com.google.protobuf.Value;
import io.pinecone.shadow.okhttp3.OkHttpClient;
import io.pinecone.shadow.okhttp3.Protocol;
import io.pinecone.unsigned_indices_model.QueryResponseWithUnsignedIndices;
import io.pinecone.unsigned_indices_model.ScoredVectorWithUnsignedIndices;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PineconeDataSource implements DataSourceProvider {

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "pinecone".equals(dataSourceConfig.get("service"));
    }

    @Data
    public static final class PineconeConfig {
        @JsonProperty(value = "api-key", required = true)
        private String apiKey;

        @JsonProperty(value = "index-name", required = true)
        private String indexName;

        @JsonProperty(value = "environment")
        @Deprecated
        private String environment;

        @JsonProperty(value = "project-name")
        @Deprecated
        private String projectName;

        private String proxy;

        @JsonProperty("server-side-timeout-sec")
        @Deprecated
        private int serverSideTimeoutSec;

        @JsonProperty("connection-timeout-seconds")
        private int connectionTimeoutSeconds = 30;
    }

    @Override
    public PineconeQueryStepDataSource createDataSourceImplementation(
            Map<String, Object> dataSourceConfig) {

        PineconeConfig clientConfig =
                ObjectMapperFactory.getDefaultMapper()
                        .convertValue(dataSourceConfig, PineconeConfig.class);

        return new PineconeQueryStepDataSource(clientConfig);
    }

    public static class PineconeQueryStepDataSource implements QueryStepDataSource {

        @Getter private final PineconeConfig clientConfig;
        private OkHttpClient httpClient;

        @Getter private Pinecone client;

        private final Map<String, Index> indexes = new ConcurrentHashMap<>();

        public PineconeQueryStepDataSource(PineconeConfig clientConfig) {
            this.clientConfig = clientConfig;
        }

        @Override
        public void initialize(Map<String, Object> config) {
            if (clientConfig.getEnvironment() != null) {
                log.warn(
                        "The 'environment' field is deprecated and will be removed in a future version. Please remove it from your configuration.");
            }
            if (clientConfig.getProjectName() != null) {
                log.warn(
                        "The 'project-name' field is deprecated and will be removed in a future version. Please remove it from your configuration.");
            }
            if (clientConfig.getServerSideTimeoutSec() != 0) {
                log.warn(
                        "The 'server-side-timeout-sec' field is deprecated and will be removed in a future version. Please remove it from your configuration.");
            }

            Pinecone.Builder builder = new Pinecone.Builder(clientConfig.getApiKey());

            int connectionTimeoutSeconds = clientConfig.getConnectionTimeoutSeconds();
            OkHttpClient.Builder httpClientBuilder =
                    new OkHttpClient.Builder()
                            .connectTimeout(connectionTimeoutSeconds, TimeUnit.SECONDS)
                            .writeTimeout(connectionTimeoutSeconds, TimeUnit.SECONDS)
                            .readTimeout(connectionTimeoutSeconds, TimeUnit.SECONDS)
                            .protocols(List.of(Protocol.HTTP_1_1))
                            .retryOnConnectionFailure(true);

            if (clientConfig.getProxy() != null) {
                int i = clientConfig.getProxy().lastIndexOf(":");
                if (i == -1) {
                    throw new IllegalArgumentException(
                            "Invalid proxy format, must be in format host:port");
                }
                String proxyHost = clientConfig.getProxy().substring(0, i);
                int proxyPort = Integer.parseInt(clientConfig.getProxy().substring(i + 1));
                Proxy proxy =
                        new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
                log.info("Using proxy: {}", proxy);
                httpClientBuilder.proxy(proxy);
            }
            httpClient = httpClientBuilder.build();

            client = builder.withOkHttpClient(httpClient).build();
        }

        public Index getIndexConnection(String indexName) {
            return indexes.computeIfAbsent(indexName, name -> client.getIndexConnection(name));
        }

        @Override
        public List<Map<String, Object>> fetchData(String query, List<Object> params) {
            try {
                Query parsedQuery = buildObjectFromJson(query, Query.class, params);
                return executeQuery(parsedQuery);
            } catch (StatusRuntimeException e) {
                throw new RuntimeException(e);
            }
        }

        private List<Map<String, Object>> executeQuery(Query parsedQuery) {
            List<Map<String, Object>> results;

            final String indexName =
                    parsedQuery.getIndex() == null
                            ? clientConfig.getIndexName()
                            : parsedQuery.getIndex();
            if (log.isDebugEnabled()) {
                log.debug("Query request on index {}: {}", indexName, parsedQuery);
            }
            if (indexName == null) {
                throw new IllegalArgumentException(
                        "index name is null. Please provide an index name in the agent with the field 'index' or in the datasource configuration");
            }

            QueryResponseWithUnsignedIndices response =
                    getIndexConnection(indexName)
                            .query(
                                    parsedQuery.getTopK(),
                                    parsedQuery.getVector(),
                                    parsedQuery.getSparseVector() == null
                                            ? null
                                            : parsedQuery.getSparseVector().getIndices(),
                                    parsedQuery.getSparseVector() == null
                                            ? null
                                            : parsedQuery.getSparseVector().getValues(),
                                    null,
                                    parsedQuery.getNamespace(),
                                    buildFilter(parsedQuery.getFilter()),
                                    parsedQuery.isIncludeValues(),
                                    parsedQuery.isIncludeMetadata());

            if (log.isDebugEnabled()) {
                log.debug("Query response: {}", response);
                List<ScoredVectorWithUnsignedIndices> matchesList = response.getMatchesList();
                for (ScoredVectorWithUnsignedIndices match : matchesList) {
                    log.debug("Match ID: {}", match.getId());
                    log.debug("Match Score: {}", match.getScore());
                    log.debug("Match Metadata: {}", match.getMetadata());
                }
            }

            log.info("Num matches: {}", response.getMatchesList().size());

            results = new ArrayList<>();
            response.getMatchesList()
                    .forEach(
                            match -> {
                                String id = match.getId();
                                Map<String, Object> row = new HashMap<>();
                                includeMetadataIfNeeded(parsedQuery, match, row);
                                row.put("id", id);
                                row.put("similarity", match.getScore());
                                results.add(row);
                            });
            return results;
        }

        private static void includeMetadataIfNeeded(
                Query parsedQuery, ScoredVectorWithUnsignedIndices match, Map<String, Object> row) {
            if (parsedQuery.includeMetadata) {
                // put all the metadata
                if (match.getMetadata() != null) {
                    match.getMetadata()
                            .getFieldsMap()
                            .forEach(
                                    (key, value) -> {
                                        if (log.isDebugEnabled()) {
                                            log.debug(
                                                    "Key: {}, value: {} {}",
                                                    key,
                                                    value,
                                                    value != null ? value.getClass() : null);
                                        }
                                        Object converted = valueToObject(value);
                                        row.put(
                                                key,
                                                converted != null ? converted.toString() : null);
                                    });
                }
            }
        }

        public static Object valueToObject(Value value) {
            if (value == null) {
                return null;
            }
            return switch (value.getKindCase()) {
                case NUMBER_VALUE -> value.getNumberValue();
                case STRING_VALUE -> value.getStringValue();
                case BOOL_VALUE -> value.getBoolValue();
                case LIST_VALUE -> value.getListValue().getValuesList().stream()
                        .map(PineconeQueryStepDataSource::valueToObject)
                        .toList();
                case STRUCT_VALUE -> value.getStructValue().getFieldsMap().entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey, e -> valueToObject(e.getValue())));
                default -> null;
            };
        }

        private static Struct buildFilter(Map<String, Object> filter) {
            if (filter == null || filter.isEmpty()) {
                return null;
            }
            Struct.Builder builder = Struct.newBuilder();
            filter.forEach(
                    (key, value) -> {
                        if (log.isDebugEnabled()) {
                            log.debug("Key: {}, value: {}", key, value);
                        }
                        if (value != null && !isNullNested(value)) {
                            builder.putFields(key, convertToValue(value));
                        }
                    });
            return builder.build();
        }

        private static boolean isNullNested(Object value) {
            if (value instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) value;
                return map.values().stream().anyMatch(v -> v == null);
            }
            return false;
        }

        @Override
        public void close() {
            for (Index value : indexes.values()) {
                value.close();
            }
            if (httpClient != null) {
                httpClient.connectionPool().evictAll();
            }
        }
    }

    /**
     * JSON model for Pinecone querys.
     *
     * <p>"vector": [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1], "filter": {"genre": {"$in": ["comedy",
     * "documentary", "drama"]}}, "topK": 1, "includeMetadata": true
     */
    @Data
    public static final class Query {

        @JsonProperty("index")
        private String index;

        @JsonProperty("vector")
        private List<Float> vector;

        @JsonProperty("filter")
        private Map<String, Object> filter;

        @JsonProperty("topK")
        private int topK = 1;

        @JsonProperty("includeMetadata")
        private boolean includeMetadata = true;

        @JsonProperty("includeValues")
        private boolean includeValues = false;

        @JsonProperty("namespace")
        private String namespace;

        @JsonProperty("sparseVector")
        private SparseVector sparseVector;
    }

    @Data
    public static final class SparseVector {
        @JsonProperty("indices")
        private List<Long> indices;

        @JsonProperty("values")
        private List<Float> values;
    }

    static Value convertToValue(Object value) {
        if (value instanceof Map) {
            Struct.Builder builder = Struct.newBuilder();
            ((Map<String, Object>) value)
                    .forEach((key, val) -> builder.putFields(key, convertToValue(val)));
            return Value.newBuilder().setStructValue(builder.build()).build();
        } else if (value instanceof String) {
            return Value.newBuilder().setStringValue(value.toString()).build();
        } else if (value instanceof Number n) {
            return Value.newBuilder().setNumberValue(n.doubleValue()).build();
        } else if (value instanceof Boolean b) {
            return Value.newBuilder().setBoolValue(b).build();
        } else if (value instanceof List list) {
            ListValue.Builder listValue = ListValue.newBuilder();
            for (Object item : list) {
                listValue.addValues(convertToValue(item));
            }
            return Value.newBuilder().setListValue(listValue).build();
        } else {
            throw new IllegalArgumentException(
                    "Unsupported value of type: "
                            + value.getClass().getName()
                            + " in Pinecone filter");
        }
    }
}
