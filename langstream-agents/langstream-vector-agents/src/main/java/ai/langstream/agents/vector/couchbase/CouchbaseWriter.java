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

import static ai.langstream.ai.agents.commons.MutableRecord.recordToMutableRecord;

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.jstl.JstlEvaluator;
import ai.langstream.api.database.VectorDatabaseWriter;
import ai.langstream.api.database.VectorDatabaseWriterProvider;
import ai.langstream.api.runner.code.Record;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.UpsertOptions;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CouchbaseWriter implements VectorDatabaseWriterProvider {

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "couchbase".equals(dataSourceConfig.get("service"));
    }

    @Override
    public CouchbaseDatabaseWriter createImplementation(Map<String, Object> datasourceConfig) {
        return new CouchbaseDatabaseWriter(datasourceConfig);
    }

    public static class CouchbaseDatabaseWriter implements VectorDatabaseWriter, AutoCloseable {

        private final Cluster cluster;
        public Collection collection;
        private JstlEvaluator idFunction;
        private JstlEvaluator vectorFunction;
        private JstlEvaluator scopeName;
        private JstlEvaluator bucketName;
        private JstlEvaluator collectionName;
        private Map<String, JstlEvaluator> metadataFunctions;

        public CouchbaseDatabaseWriter(Map<String, Object> datasourceConfig) {
            String username = (String) datasourceConfig.get("username");
            String password = (String) datasourceConfig.get("password");
            String connectionString = (String) datasourceConfig.get("connection-string");

            // Create a cluster with the WAN profile
            ClusterOptions clusterOptions =
                    ClusterOptions.clusterOptions(username, password)
                            .environment(
                                    env -> {
                                        env.applyProfile("wan-development");
                                    });

            cluster = Cluster.connect(connectionString, clusterOptions);
        }

        @Override
        public void close() throws Exception {
            cluster.disconnect();
        }

        @Override
        public void initialise(Map<String, Object> agentConfiguration) throws Exception {

            this.idFunction = buildEvaluator(agentConfiguration, "vector.id", String.class);
            this.vectorFunction = buildEvaluator(agentConfiguration, "vector.vector", List.class);
            this.bucketName = buildEvaluator(agentConfiguration, "bucket-name", String.class);
            this.scopeName = buildEvaluator(agentConfiguration, "scope-name", String.class);
            this.collectionName =
                    buildEvaluator(agentConfiguration, "collection-name", String.class);
            this.metadataFunctions = new HashMap<>();
            agentConfiguration.forEach(
                    (key, value) -> {
                        if (key.startsWith("record.")) {
                            String metadataKey = key.substring("record.".length());
                            metadataFunctions.put(
                                    metadataKey,
                                    buildEvaluator(agentConfiguration, key, Object.class));
                        }
                    });
        }

        @Override
        public CompletableFuture<Void> upsert(Record record, Map<String, Object> context) {

            CompletableFuture<Void> handle = new CompletableFuture<>();
            try {

                MutableRecord mutableRecord = recordToMutableRecord(record, true);

                // Evaluate the ID using the idFunction
                String docId =
                        idFunction != null ? (String) idFunction.evaluate(mutableRecord) : null;

                if (docId == null) {
                    throw new IllegalArgumentException("docId is null, cannot upsert document");
                }

                String bucketS =
                        bucketName != null ? (String) bucketName.evaluate(mutableRecord) : null;
                String scopeS =
                        scopeName != null ? (String) scopeName.evaluate(mutableRecord) : null;
                String collectionS =
                        collectionName != null
                                ? (String) collectionName.evaluate(mutableRecord)
                                : null;

                // Get the bucket, scope, and collection
                Bucket bucket = cluster.bucket(bucketS);
                bucket.waitUntilReady(Duration.ofSeconds(10));

                Scope scope = bucket.scope(scopeS);
                collection = scope.collection(collectionS);

                // Prepare the content map
                Map<String, Object> content = new HashMap<>();

                // Add the vector embedding
                List<?> vector =
                        vectorFunction != null
                                ? (List<?>) vectorFunction.evaluate(mutableRecord)
                                : null;
                if (vector != null) {
                    content.put("vector", vector);
                }

                // Add metadata
                metadataFunctions.forEach(
                        (key, evaluator) -> {
                            Object value = evaluator.evaluate(mutableRecord);
                            if (value != null) content.put(key, value);
                        });

                // Perform the upsert
                MutationResult result =
                        collection.upsert(docId, content, UpsertOptions.upsertOptions());

                // Logging the result of the upsert operation
                log.info("Upsert successful for document ID '{}'", docId);

                handle.complete(null); // Completing the future successfully
            } catch (Exception e) {
                log.error("Failed to upsert document with ID '{}'", record.key(), e);
                handle.completeExceptionally(e); // Completing the future exceptionally
            }

            return handle;
        }
    }

    public static JstlEvaluator buildEvaluator(
            Map<String, Object> agentConfiguration, String param, Class type) {
        String expression = agentConfiguration.getOrDefault(param, "").toString();
        if (expression == null || expression.isEmpty()) {
            return null;
        }
        return new JstlEvaluator("${" + expression + "}", type);
    }
}
