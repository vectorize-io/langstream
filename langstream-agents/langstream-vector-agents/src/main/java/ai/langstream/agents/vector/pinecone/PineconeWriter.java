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

import static ai.langstream.ai.agents.commons.MutableRecord.recordToMutableRecord;

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.jstl.JstlEvaluator;
import ai.langstream.api.database.VectorDatabaseWriter;
import ai.langstream.api.database.VectorDatabaseWriterProvider;
import ai.langstream.api.runner.code.Record;
import io.pinecone.proto.UpsertResponse;
import io.pinecone.shadow.com.google.protobuf.Struct;
import io.pinecone.shadow.com.google.protobuf.Value;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PineconeWriter implements VectorDatabaseWriterProvider {

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "pinecone".equals(dataSourceConfig.get("service"));
    }

    @Override
    public VectorDatabaseWriter createImplementation(Map<String, Object> datasourceConfig) {
        return new PineconeVectorDatabaseWriter(datasourceConfig);
    }

    private static class PineconeVectorDatabaseWriter implements VectorDatabaseWriter {

        private JstlEvaluator idFunction;
        private JstlEvaluator namespaceFunction;
        private JstlEvaluator vectorFunction;
        private Map<String, JstlEvaluator> metadataFunctions;
        private final PineconeDataSource.PineconeQueryStepDataSource dataSource;

        public PineconeVectorDatabaseWriter(Map<String, Object> datasourceConfig) {
            PineconeDataSource dataSourceProvider = new PineconeDataSource();
            dataSource = dataSourceProvider.createDataSourceImplementation(datasourceConfig);
        }

        @Override
        public void initialise(Map<String, Object> agentConfiguration) {
            dataSource.initialize(null);

            this.idFunction = buildEvaluator(agentConfiguration, "vector.id", String.class);
            this.vectorFunction = buildEvaluator(agentConfiguration, "vector.vector", List.class);
            this.namespaceFunction =
                    buildEvaluator(agentConfiguration, "vector.namespace", String.class);

            this.metadataFunctions = new HashMap<>();
            agentConfiguration.forEach(
                    (key, value) -> {
                        if (key.startsWith("vector.metadata.")) {
                            String metadataKey = key.substring("vector.metadata.".length());
                            metadataFunctions.put(
                                    metadataKey,
                                    buildEvaluator(agentConfiguration, key, Object.class));
                        }
                    });
        }

        @Override
        public CompletableFuture<?> upsert(Record record, Map<String, Object> context) {
            CompletableFuture<?> handle = new CompletableFuture<>();
            try {
                MutableRecord mutableRecord = recordToMutableRecord(record, true);
                String id = idFunction != null ? (String) idFunction.evaluate(mutableRecord) : null;
                String namespace =
                        namespaceFunction != null
                                ? (String) namespaceFunction.evaluate(mutableRecord)
                                : null;
                List<Object> vector =
                        vectorFunction != null
                                ? (List<Object>) vectorFunction.evaluate(mutableRecord)
                                : null;

                Map<String, Object> metadata =
                        metadataFunctions.entrySet().stream()
                                .filter(e -> e.getValue() != null) // Ensure the evaluator itself is
                                // not null
                                .map(
                                        e -> {
                                            Object value = e.getValue().evaluate(mutableRecord);
                                            return value != null
                                                    ? Map.entry(e.getKey(), value)
                                                    : null;
                                        })
                                .filter(entry -> entry != null) // Filter out null entries after
                                // evaluation
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                Map<String, Value> metadataFields = new HashMap<>();
                for (Map.Entry<String, Object> meta : metadata.entrySet()) {
                    metadataFields.put(
                            meta.getKey(), PineconeDataSource.convertToValue(meta.getValue()));
                }
                Struct metadataStruct = Struct.newBuilder().putAllFields(metadataFields).build();

                List<Float> vectorFloat = null;
                if (vector != null) {
                    vectorFloat =
                            vector.stream()
                                    .map(
                                            n -> {
                                                if (n instanceof String s) {
                                                    return Float.parseFloat(s);
                                                } else if (n instanceof Number u) {
                                                    return u.floatValue();
                                                } else {
                                                    throw new IllegalArgumentException(
                                                            "only vectors of floats are supported");
                                                }
                                            })
                                    .collect(Collectors.toList());
                }
                UpsertResponse upsertResponse =
                        dataSource
                                .getIndexConnection(dataSource.getClientConfig().getIndexName())
                                .upsert(id, vectorFloat, null, null, metadataStruct, namespace);
                log.info("Result {}", upsertResponse);
                handle.complete(null);
            } catch (Exception e) {
                handle.completeExceptionally(e);
            }
            return handle;
        }
    }

    private static JstlEvaluator buildEvaluator(
            Map<String, Object> agentConfiguration, String param, Class type) {
        String expression = agentConfiguration.getOrDefault(param, "").toString();
        if (expression == null || expression.isEmpty()) {
            return null;
        }
        return new JstlEvaluator("${" + expression + "}", type);
    }
}
