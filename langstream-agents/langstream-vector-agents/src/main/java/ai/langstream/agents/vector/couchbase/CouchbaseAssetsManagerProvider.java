package ai.langstream.agents.vector.couchbase;

import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.assets.AssetManagerProvider;
import ai.langstream.api.util.ConfigurationUtils;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.manager.bucket.BucketSettings;
import com.couchbase.client.java.manager.bucket.BucketType;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CouchbaseAssetsManagerProvider implements AssetManagerProvider {

    @Override
    public boolean supports(String assetType) {
        return "couchbase-assets".equals(assetType);
    }

    @Override
    public AssetManager createInstance(String assetType) {
        switch (assetType) {
            case "couchbase-assets":
                return new CouchbaseAssetsManager();
            default:
                throw new IllegalArgumentException();
        }
    }

    public static class CouchbaseAssetsManager implements AssetManager {
        private Cluster cluster;
        private Bucket bucket;
        private Scope scope;
        private Collection collection;
        private String bucketName;
        private String scopeName;
        private String collectionName;
        private AssetDefinition assetDefinition;
        private String indexName;
        private String username;
        private String password;
        private String connectionString;
        private String port;

        @Override
        public void initialize(AssetDefinition assetDefinition) throws Exception {
            this.assetDefinition = assetDefinition;
            Map<String, Object> configuration = assetDefinition.getConfig();
            this.username =
                    ConfigurationUtils.requiredField(
                            configuration, "username", () -> "couchbase asset");
            this.password =
                    ConfigurationUtils.requiredField(
                            configuration, "password", () -> "couchbase asset");
            this.connectionString =
                    ConfigurationUtils.requiredField(
                            configuration, "connection-string", () -> "couchbase asset");
            this.bucketName =
                    ConfigurationUtils.requiredField(
                            configuration, "bucket-name", () -> "couchbase asset");
            this.scopeName =
                    ConfigurationUtils.requiredField(
                            configuration, "scope-name", () -> "couchbase asset");
            this.collectionName =
                    ConfigurationUtils.requiredField(
                            configuration, "collection-name", () -> "couchbase asset");
            this.port =
                    ConfigurationUtils.requiredField(
                            configuration, "port", () -> "couchbase asset");

            cluster =
                    Cluster.connect(
                            connectionString, ClusterOptions.clusterOptions(username, password));

            bucket = cluster.bucket(bucketName);
        }

        @Override
        public boolean assetExists() throws Exception {
            try {
                // Retrieve the list of collections in the specified scope
                List<CollectionSpec> collections =
                        bucket.collections().getAllScopes().stream()
                                .filter(scope -> scope.name().equals(scopeName))
                                .flatMap(scope -> scope.collections().stream())
                                .collect(Collectors.toList());

                // Check if the desired collection exists in the list
                for (CollectionSpec collectionSpec : collections) {
                    if (collectionSpec.name().equals(collectionName)) {
                        return true;
                    }
                }
                return false;
            } catch (Exception e) {
                // Handle exceptions if any issues occur
                e.printStackTrace();
                return false;
            }
        }

        @Override
        public void deployAsset() throws Exception {
            // Check and create bucket if it doesn't exist
            if (!bucketExists()) {
                System.out.println("Creating bucket " + bucketName);
                cluster.buckets()
                        .createBucket(
                                BucketSettings.create(bucketName)
                                        .bucketType(BucketType.COUCHBASE)
                                        .ramQuotaMB(100)); // Specify the RAM size
            }

            bucket = cluster.bucket(bucketName);

            // Check and create scope if it doesn't exist
            if (!scopeExists()) {
                System.out.println("Creating scope " + scopeName);
                bucket.collections().createScope(scopeName);
            }

            // Check and create collection if it doesn't exist
            if (!assetExists()) {
                System.out.println("Creating collection " + collectionName);
                bucket.collections().createCollection(scopeName, collectionName);
            }

            int vectorDimension = getVectorDimension();

            System.out.println(
                    "Creating vector search index for collection "
                            + collectionName
                            + "connection string"
                            + connectionString);
            createVectorSearchIndex(
                    scopeName, vectorDimension, username, password, connectionString, port);
        }

        private boolean bucketExists() {
            try {
                cluster.buckets().getBucket(bucketName);
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        private boolean scopeExists() {
            try {
                return bucket.collections().getAllScopes().stream()
                        .anyMatch(scope -> scope.name().equals(scopeName));
            } catch (Exception e) {
                return false;
            }
        }

        private void createVectorSearchIndex(
                String scopeName,
                int vectorDimension,
                String username,
                String password,
                String connectionString,
                String port)
                throws IOException, InterruptedException {
            String indexLabel = "vectorize-index";
            String indexName = bucketName + "." + scopeName + "." + indexLabel;
            String indexDefinition =
                    "{\n"
                            + "  \"type\": \"fulltext-index\",\n"
                            + "  \"name\": \""
                            + indexLabel
                            + "\",\n"
                            + "  \"sourceType\": \"gocbcore\",\n"
                            + "  \"sourceName\": \"testbucket\",\n"
                            + "  \"planParams\": {\"maxPartitionsPerPIndex\": 512},\n"
                            + "  \"params\": {\n"
                            + "    \"doc_config\": {\n"
                            + "      \"mode\": \"scope.collection.type_field\",\n"
                            + "      \"type_field\": \"type\"\n"
                            + "    },\n"
                            + "    \"mapping\": {\n"
                            + "      \"analysis\": {},\n"
                            + "      \"default_analyzer\": \"standard\",\n"
                            + "      \"default_datetime_parser\": \"dateTimeOptional\",\n"
                            + "      \"default_field\": \"_all\",\n"
                            + "      \"default_mapping\": {\"dynamic\": false, \"enabled\": false},\n"
                            + "      \"types\": {\n"
                            + "        \""
                            + scopeName
                            + "."
                            + collectionName
                            + "\": {\n"
                            + "          \"dynamic\": false,\n"
                            + "          \"enabled\": true,\n"
                            + "          \"properties\": {\n"
                            + "            \"vector\": {\n"
                            + "              \"fields\": [{\"dims\":"
                            + vectorDimension
                            + ", \"index\": true, \"name\": \"vector\", \"similarity\": \"dot_product\", \"type\": \"vector\"}]\n"
                            + "            },\n"
                            + "            \"vecPlanId\": {\n"
                            + "              \"fields\": [{\"index\": true, \"store\": true, \"name\": \"vecPlanId\", \"type\": \"text\"}]\n"
                            + "            }\n"
                            + "          }\n"
                            + "        }\n"
                            + "      }\n"
                            + "    }\n"
                            + "  }\n"
                            + "}";

            System.out.println(
                    "Creating vector search index " + indexName + " on host " + connectionString);

            String host = connectionString.replace("couchbase://", "").split(":")[0];
            System.out.println("Extracted host: " + host);

            String urlStr =
                    "http://"
                            + host
                            + ":"
                            + port
                            + "/api/bucket/"
                            + bucketName
                            + "/scope/"
                            + scopeName
                            + "/index/"
                            + indexLabel;

            System.out.println("Constructed URL: " + urlStr);
            URL url = new URL(urlStr);
            HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
            httpConn.setDoOutput(true);
            httpConn.setRequestMethod("PUT");
            httpConn.setRequestProperty("Content-Type", "application/json");
            httpConn.setRequestProperty(
                    "Authorization",
                    "Basic "
                            + Base64.getEncoder()
                                    .encodeToString(
                                            (username + ":" + password)
                                                    .getBytes(StandardCharsets.UTF_8)));
            httpConn.getOutputStream().write(indexDefinition.getBytes(StandardCharsets.UTF_8));
            httpConn.getOutputStream().flush();
            httpConn.getOutputStream().close();

            int responseCode = httpConn.getResponseCode();
            if (responseCode != 200) {
                InputStream errorStream = httpConn.getErrorStream();
                String errorMessage =
                        new BufferedReader(new InputStreamReader(errorStream))
                                .lines()
                                .collect(Collectors.joining("\n"));
                throw new IOException(
                        "Failed to create index: HTTP response code "
                                + responseCode
                                + ", message: "
                                + errorMessage);
            }
        }

        @Override
        public boolean deleteAssetIfExists() throws Exception {
            try {
                if (assetExists()) {
                    System.out.println(
                            "Deleting collection " + collectionName + " in scope " + scopeName);
                    bucket.collections().dropCollection(scopeName, collectionName);
                    return true;
                }
                return false;
            } catch (Exception e) {
                // Handle exceptions if any issues occur
                e.printStackTrace();
                return false;
            }
        }

        @Override
        public void close() {
            if (cluster != null) {
                System.out.println("Closing Couchbase cluster connection");
                cluster.disconnect();
            }
        }

        private int getVectorDimension() {
            return ConfigurationUtils.getInt("vector-dimension", 1536, assetDefinition.getConfig());
        }
    }
}
