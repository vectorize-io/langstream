package ai.langstream.apigateway.auth.common.store;

import io.minio.DownloadObjectArgs;
import io.minio.MinioClient;
import io.minio.errors.ErrorResponseException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static ai.langstream.api.util.ConfigurationUtils.getString;

@Slf4j
public class S3RevokedTokensStore implements RevokedTokensStore {

    private final MinioClient minioClient;
    private final String bucketName;
    private final String objectName;

    private volatile Set<String> revokedTokens = new HashSet<>();

    public S3RevokedTokensStore(Map<String, Object> config) {
        bucketName =
                getString(
                        "s3-bucket", null,
                        config);

        objectName =
                getString(
                        "s3-object", null,
                        config);
        if (bucketName == null || objectName == null) {
            throw new IllegalArgumentException("S3 bucket and object name must be provided");
        }
        final String endpoint =
                getString(
                        "s3-endpoint",
                        null,
                        config);
        final String username =
                getString("s3-access-key", null, config);
        final String password =
                getString("s3-secret-key", null, config);
        final String region = getString("s3-region", "", config);

        log.info(
                "Connecting to S3 Bucket at {} in region {} with user {}",
                endpoint,
                region,
                username);

        MinioClient.Builder builder =
                MinioClient.builder().endpoint(endpoint).credentials(username, password);
        if (!region.isBlank()) {
            builder.region(region);
        }
        minioClient = builder.build();
    }

    @Override
    public boolean isTokenRevoked(String token) {
        synchronized (this) {
            return revokedTokens.contains(token);
        }
    }

    @Override
    @SneakyThrows
    public void refreshRevokedTokens() {
        Set<String> temporaryRevokedTokens = readRevokedTokensFromObject();
        if (temporaryRevokedTokens == null) {
            return;
        }
        log.info("refreshed revoked tokens, new count {}, old count {}", temporaryRevokedTokens.size(), revokedTokens.size());
        synchronized (this) {
            revokedTokens = temporaryRevokedTokens;
        }
    }

    private Set<String> readRevokedTokensFromObject() throws IOException {
        Path tempDirectory = Files.createTempDirectory("revoked-tokens");
        Path tempFile = tempDirectory.resolve("revoked-tokens.txt");
        Set<String> temporaryRevokedTokens = new HashSet<>();
        try {
            DownloadObjectArgs downloadObjectArgs = DownloadObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .filename(tempFile.toAbsolutePath().toString())
                    .build();
            minioClient.downloadObject(downloadObjectArgs);
            try (BufferedReader br = new BufferedReader(new FileReader(tempFile.toFile()))) {
                String line;
                while ((line = br.readLine()) != null) {
                    temporaryRevokedTokens.add(line);
                }
            }
        } catch (ErrorResponseException e) {
            if (e.errorResponse().code().equals("NoSuchKey")) {
                log.warn("No revoked tokens file found in S3");
                return null;
            } else {
                log.error("Error downloading revoked tokens from S3", e);
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            tempFile.toFile().delete();
            tempDirectory.toFile().delete();
        }
        return temporaryRevokedTokens;
    }

    @Override
    public void close() throws Exception {
        if(minioClient != null) {
            minioClient.close();
        }
    }
}
