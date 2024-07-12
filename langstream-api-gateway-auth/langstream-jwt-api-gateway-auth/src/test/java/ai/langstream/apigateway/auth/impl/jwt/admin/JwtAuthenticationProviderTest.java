package ai.langstream.apigateway.auth.impl.jwt.admin;

import ai.langstream.api.gateway.GatewayAuthenticationResult;
import ai.langstream.api.gateway.GatewayRequestContext;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Gateway;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.jsonwebtoken.Jwts;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import lombok.SneakyThrows;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.spec.RSAPublicKeySpec;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
@Testcontainers
class JwtAuthenticationProviderTest {


    @RegisterExtension
    WireMockExtension wm1 =
            WireMockExtension.newInstance()
                    .options(wireMockConfig().dynamicPort())
                    .failOnUnmatchedRequests(true)
                    .build();

    KeyPair kp;
    private static final String JWKS_PATH = "/auth/.well-known/jwks.json";

    private static final DockerImageName localstackImage =
            DockerImageName.parse("localstack/localstack:2.2.0");

    @Container
    private static final LocalStackContainer localstack =
            new LocalStackContainer(localstackImage).withServices(S3);


    @BeforeEach
    public void beforeEach() throws Exception {
        genAndExposeKeyPair();
    }

    @Test
    @SneakyThrows
    public void test() {
        MinioClient minioClient = MinioClient.builder()
                .endpoint(localstack.getEndpointOverride(S3).toString())
                .build();

        AtomicInteger refreshCount = new AtomicInteger(0);

        JwtAuthenticationProvider provider = new JwtAuthenticationProvider() {
            @Override
            public void onRevokedTokensRefreshed() {
                refreshCount.incrementAndGet();
            }
        };
        provider.initialize(Map.of(
                "jwks-hosts-allowlist", "localhost",
                "auth-claim", "iss",
                "admin-roles", List.of("testrole"),
                "revoked-tokens-store", Map.of("type", "s3", "refresh-period-seconds", 1,
                        "config",
                        Map.of("s3-bucket", "testbucket", "s3-object", "bad-tokens.txt", "s3-endpoint", localstack.getEndpointOverride(S3).toString(),
                                "s3-access-key", "minioadmin", "s3-secret-key", "minioadmin"))
        ));

        final String goodToken = Jwts.builder()
                .claim("iss", "testrole")
                .claim("jwks_uri", wm1.url(JWKS_PATH))
                .signWith(kp.getPrivate())
                .compact();
        assertTrue(provider.authenticate(newContext(goodToken))
                .authenticated()
        );

        assertFalse(provider.authenticate(newContext(Jwts.builder()
                        .claim("iss", "testrole_no")
                        .claim("jwks_uri", wm1.url(JWKS_PATH))
                        .signWith(kp.getPrivate())
                        .compact()))
                .authenticated()
        );

        minioClient.makeBucket(MakeBucketArgs.builder().bucket("testbucket").build());
        final int before = refreshCount.get();

        String tokenList = "another\n" + goodToken + "\n";
        minioClient.putObject(PutObjectArgs.builder()
                .bucket("testbucket")
                .object("bad-tokens.txt")
                .stream(new ByteArrayInputStream(tokenList.getBytes(StandardCharsets.UTF_8)), tokenList.length(), -1)
                .build());
        Awaitility.await()
                .untilAsserted(() -> assertTrue(refreshCount.get() > before));

        assertFalse(provider.authenticate(newContext(goodToken))
                .authenticated()
        );
    }

    @NotNull
    private static GatewayRequestContext newContext(String credentials) {
        return new GatewayRequestContext() {
            @Override
            public String tenant() {
                throw new UnsupportedOperationException();
            }

            @Override
            public String applicationId() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Application application() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Gateway gateway() {
                throw new UnsupportedOperationException();
            }

            @Override
            public String credentials() {
                return credentials;
            }

            @Override
            public boolean isTestMode() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Map<String, String> userParameters() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Map<String, String> options() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Map<String, String> httpHeaders() {
                throw new UnsupportedOperationException();
            }
        };
    }


    private void genAndExposeKeyPair() throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        kp = kpg.generateKeyPair();
        final RSAPublicKeySpec spec =
                KeyFactory.getInstance("RSA").getKeySpec(kp.getPublic(), RSAPublicKeySpec.class);

        final byte[] e =
                Base64.getUrlEncoder()
                        .withoutPadding()
                        .encode(spec.getPublicExponent().toByteArray());
        final byte[] mod =
                Base64.getUrlEncoder().withoutPadding().encode(spec.getModulus().toByteArray());
        wm1.stubFor(
                WireMock.get(JWKS_PATH)
                        .willReturn(
                                WireMock.okJson(
                                        """
                                                {"keys":[{"alg":"RS256","e":"%s","kid":"1","kty":"RSA","n":"%s"}]}
                                                """
                                                .formatted(
                                                        new String(e, StandardCharsets.UTF_8),
                                                        new String(mod, StandardCharsets.UTF_8)))));
    }

}