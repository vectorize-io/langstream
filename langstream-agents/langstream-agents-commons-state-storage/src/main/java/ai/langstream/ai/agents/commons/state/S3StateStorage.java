package ai.langstream.ai.agents.commons.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.*;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.MinioException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class S3StateStorage<T> implements StateStorage<T> {

    public static String computeObjectName(
            final String tenant,
            final String globalAgentId,
            final Map<String, Object> agentConfiguration,
            final String suffix) {
        final boolean prependTenant = StateStorage.isFilePrependTenant(agentConfiguration);
        final String prefix = StateStorage.getFilePrefix(agentConfiguration);

        final String pathPrefix;
        if (prependTenant) {
            pathPrefix = prefix + tenant + "/" + globalAgentId;
        } else {
            pathPrefix = prefix + globalAgentId;
        }
        return pathPrefix + "." + suffix + ".status.json";
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final MinioClient minioClient;
    private final String bucketName;
    private final String objectName;

    public S3StateStorage(MinioClient minioClient, String bucketName, String objectName) {
        this.minioClient = minioClient;
        this.bucketName = bucketName;
        this.objectName = objectName;
        makeBucketIfNotExists();
    }

    @SneakyThrows
    private void makeBucketIfNotExists() {
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
            log.info("Creating bucket {}", bucketName);
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
        } else {
            log.info("Bucket {} already exists", bucketName);
        }
    }

    @Override
    public void store(T status) throws Exception {
        byte[] content = MAPPER.writeValueAsBytes(status);
        log.info("Storing state in {}, {} bytes", objectName, content.length);
        putWithRetries(
                () ->
                        PutObjectArgs.builder()
                                .bucket(bucketName)
                                .object(objectName)
                                .contentType("text/json")
                                .stream(new ByteArrayInputStream(content), content.length, -1)
                                .build());
    }

    private void putWithRetries(Supplier<PutObjectArgs> args)
            throws MinioException, NoSuchAlgorithmException, InvalidKeyException, IOException {
        putWithRetries(minioClient, args);
    }

    public static void putWithRetries(MinioClient minioClient, Supplier<PutObjectArgs> args)
            throws MinioException, NoSuchAlgorithmException, InvalidKeyException, IOException {
        int attempt = 0;
        int maxRetries = 5;
        while (attempt < maxRetries) {
            try {
                attempt++;
                log.info("attempting to put object to s3 {}/{}", attempt, maxRetries);
                minioClient.putObject(args.get());
                return;
            } catch (IOException e) {
                log.error("error putting object to s3", e);
                if (e.getMessage() != null && e.getMessage().contains("unexpected end of stream")
                        || e.getMessage().contains("unexpected EOF")
                        || e.getMessage().contains("Broken pipe")) {
                    if (attempt == maxRetries) {
                        throw e;
                    }
                    long backoffTime = (long) Math.pow(2, attempt - 1) * 2000;
                    log.info(
                            "retrying put due to unexpected end of stream, retrying in {} ms",
                            backoffTime);
                    try {
                        TimeUnit.MILLISECONDS.sleep(backoffTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(ie);
                    }
                } else {
                    throw e;
                }
            }
        }
    }

    @Override
    public T get(Class<T> clazz) throws Exception {
        try {
            GetObjectResponse result =
                    minioClient.getObject(
                            GetObjectArgs.builder().bucket(bucketName).object(objectName).build());
            byte[] content = result.readAllBytes();
            log.info("Restoring state from {}, {} bytes", objectName, content.length);
            try {
                return MAPPER.readValue(content, clazz);
            } catch (IOException e) {
                log.error("Error parsing state file", e);
                return null;
            }
        } catch (ErrorResponseException e) {
            if (e.errorResponse().code().equals("NoSuchKey")) {
                return null;
            }
            throw e;
        }
    }

    @Override
    public String getStateReference() {
        return "s3://" + bucketName + "/" + objectName;
    }
}
