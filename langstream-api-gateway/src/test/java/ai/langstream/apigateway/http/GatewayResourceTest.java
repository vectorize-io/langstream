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
package ai.langstream.apigateway.http;

import static ai.langstream.apigateway.ApiGatewayTestUtil.findMetric;
import static ai.langstream.apigateway.ApiGatewayTestUtil.getPrometheusMetrics;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import ai.langstream.api.model.*;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.DeployContext;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.apigateway.ApiGatewayTestUtil;
import ai.langstream.apigateway.api.ConsumePushMessage;
import ai.langstream.apigateway.config.GatewayTestAuthenticationProperties;
import ai.langstream.apigateway.runner.TopicConnectionsRuntimeProviderBean;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.parser.ModelBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "spring.main.allow-bean-definition-overriding=true",
        })
@WireMockTest
@Slf4j
@AutoConfigureObservability
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
abstract class GatewayResourceTest {

    public static final Path agentsDirectory;
    protected static final HttpClient CLIENT = HttpClient.newHttpClient();

    static {
        agentsDirectory = Path.of(System.getProperty("user.dir"), "target", "agents");
        log.info("Agents directory is {}", agentsDirectory);
    }

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    static List<TopicWithSchema> topics;
    ExecutorService futuresExecutor;
    static Gateways testGateways;

    protected static ApplicationStore getMockedStore(String instanceYaml) {
        wireMock.register(
                WireMock.get("/agent-endpoint/custom-path")
                        .willReturn(WireMock.ok("agent response")));

        wireMock.register(
                WireMock.get("/agent-endpoint").willReturn(WireMock.ok("agent response ROOT")));
        ApplicationStore mock = Mockito.mock(ApplicationStore.class);
        doAnswer(
                        invocationOnMock -> {
                            final StoredApplication storedApplication = new StoredApplication();
                            final Application application = buildApp(instanceYaml);
                            storedApplication.setInstance(application);
                            return storedApplication;
                        })
                .when(mock)
                .get(anyString(), anyString(), anyBoolean());
        doAnswer(
                        invocationOnMock ->
                                ApplicationSpecs.builder()
                                        .application(buildApp(instanceYaml))
                                        .build())
                .when(mock)
                .getSpecs(anyString(), anyString());

        return mock;
    }

    protected static GatewayTestAuthenticationProperties getGatewayTestAuthenticationProperties() {
        final GatewayTestAuthenticationProperties props = new GatewayTestAuthenticationProperties();
        props.setType("http");
        props.setConfiguration(
                Map.of(
                        "base-url",
                        wireMockBaseUrl,
                        "path-template",
                        "/auth/{tenant}",
                        "headers",
                        Map.of("h1", "v1")));
        return props;
    }

    @Autowired private TopicConnectionsRuntimeProviderBean topicConnectionsRuntimeProvider;

    @NotNull
    private static Application buildApp(String instanceYaml) throws Exception {
        final Map<String, Object> module =
                Map.of(
                        "module",
                        "mod1",
                        "id",
                        "p",
                        "topics",
                        topics.stream()
                                .map(
                                        t -> {
                                            Map<String, Object> map = new HashMap<>();
                                            map.put("name", t.topic());
                                            map.put("creation-mode", "create-if-not-exists");
                                            if (t.schemaType() != null) {
                                                map.put(
                                                        "schema",
                                                        Map.of(
                                                                "type", t.schemaType(),
                                                                "schema", t.schemaDef()));
                                            }
                                            return map;
                                        })
                                .collect(Collectors.toList()));

        final Application application =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        new ObjectMapper(new YAMLFactory())
                                                .writeValueAsString(module)),
                                instanceYaml,
                                null)
                        .getApplication();
        application.setGateways(testGateways);
        return application;
    }

    @LocalServerPort int port;

    @Autowired MockMvc mockMvc;
    @Autowired ApplicationStore store;

    static WireMock wireMock;
    static String wireMockBaseUrl;
    static AtomicInteger topicCounter = new AtomicInteger();

    private static String genTopic(String testName) {
        return testName + "-topic" + topicCounter.incrementAndGet();
    }

    @BeforeAll
    public static void beforeAll(WireMockRuntimeInfo wmRuntimeInfo) {
        wireMock = wmRuntimeInfo.getWireMock();
        wireMockBaseUrl = wmRuntimeInfo.getHttpBaseUrl();
    }

    @BeforeEach
    public void beforeEach(WireMockRuntimeInfo wmRuntimeInfo, TestInfo testInfo) {
        testGateways = null;
        topics = null;
        Awaitility.setDefaultTimeout(30, TimeUnit.SECONDS);
        futuresExecutor =
                Executors.newCachedThreadPool(
                        new BasicThreadFactory.Builder()
                                .namingPattern("test-exec-" + testInfo.getDisplayName() + "-%d")
                                .build());
    }

    @AfterAll
    public static void afterAll() {
        Awaitility.reset();
    }

    @AfterEach
    public void afterEach() throws Exception {
        Metrics.globalRegistry.clear();
        futuresExecutor.shutdownNow();
        futuresExecutor.awaitTermination(1, TimeUnit.MINUTES);
        futuresExecutor = null;
    }

    @SneakyThrows
    void produceJsonAndExpectOk(String url, String content) {
        produceJsonAndExpectOk(url, content, Map.of());
    }

    @SneakyThrows
    void produceJsonAndExpectOk(String url, String content, Map<String, String> headers) {
        final HttpResponse<String> response = sendRequest(url, content, headers);
        assertEquals(200, response.statusCode());
        assertEquals("""
                {"status":"OK","reason":null}""", response.body());
    }

    @SneakyThrows
    String produceTextAndGetBody(String url, String content) {
        final HttpRequest request =
                HttpRequest.newBuilder(URI.create(url))
                        .POST(HttpRequest.BodyPublishers.ofString(content))
                        .build();
        final HttpResponse<String> response =
                CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        return response.body();
    }

    @SneakyThrows
    String produceJsonAndGetBody(String url, String content) {
        final HttpRequest request =
                HttpRequest.newBuilder(URI.create(url))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(content))
                        .build();
        final HttpResponse<String> response =
                CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        return response.body();
    }

    @SneakyThrows
    void produceJsonAndExpectBadRequest(String url, String content, String errorMessage) {
        produceJsonAndExpectBadRequest(url, content, Map.of(), errorMessage);
    }

    @SneakyThrows
    void produceJsonAndExpectBadRequest(
            String url, String content, Map<String, String> headers, String errorMessage) {
        HttpResponse<String> response = sendRequest(url, content, headers);
        assertEquals(400, response.statusCode());
        log.info("Response body: {}", response.body());
        final Map map = new ObjectMapper().readValue(response.body(), Map.class);
        String detail = (String) map.get("detail");
        assertTrue(detail.contains(errorMessage));
    }

    @SneakyThrows
    void produceJsonAndExpectUnauthorized(String url, String content) {
        produceJsonAndExpectUnauthorized(url, content, Map.of());
    }

    @SneakyThrows
    void produceJsonAndExpectUnauthorized(String url, String content, Map<String, String> headers) {
        final HttpResponse<String> response = sendRequest(url, content, headers);
        assertEquals(401, response.statusCode());
        log.info("Response body: {}", response.body());
    }

    private static HttpResponse<String> sendRequest(
            String url, String content, Map<String, String> headers)
            throws IOException, InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url));
        headers.forEach((key, value) -> builder.header(key, value));
        builder.header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(content))
                .build();
        final HttpResponse<String> response =
                CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        return response;
    }

    @Test
    void testSimpleProduce() throws Exception {
        final String topic = genTopic("testSimpleProduce");
        prepareTopicsForTest(topic);
        testGateways =
                new Gateways(
                        List.of(
                                Gateway.builder()
                                        .id("produce")
                                        .type(Gateway.GatewayType.produce)
                                        .topic(topic)
                                        .build(),
                                Gateway.builder()
                                        .id("produce-value")
                                        .type(Gateway.GatewayType.produce)
                                        .topic(topic)
                                        .produceOptions(
                                                new Gateway.ProduceOptions(
                                                        null, Gateway.ProducePayloadSchema.value))
                                        .build(),
                                Gateway.builder()
                                        .id("consume")
                                        .type(Gateway.GatewayType.consume)
                                        .topic(topic)
                                        .build()));

        final String url =
                "http://localhost:%d/api/gateways/produce/tenant1/application1/produce"
                        .formatted(port);
        produceJsonAndExpectOk(url, "{\"key\": \"my-key\", \"value\": \"my-value\"}");
        produceJsonAndExpectOk(url, "{\"key\": \"my-key\"}");
        produceJsonAndExpectOk(url, "{\"key\": \"my-key\", \"headers\": {\"h1\": \"v1\"}}");
        HttpRequest request =
                HttpRequest.newBuilder(URI.create(url))
                        .POST(HttpRequest.BodyPublishers.ofString("my-string"))
                        .build();
        HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertEquals("""
                {"status":"OK","reason":null}""", response.body());

        request =
                HttpRequest.newBuilder(URI.create(url))
                        .header("Content-Type", "text/plain")
                        .POST(HttpRequest.BodyPublishers.ofString("my-string"))
                        .build();
        response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        log.info("Response body: {}", response.body());
        assertEquals(200, response.statusCode());
        assertEquals("""
                {"status":"OK","reason":null}""", response.body());

        final String urlValue =
                "http://localhost:%d/api/gateways/produce/tenant1/application1/produce-value"
                        .formatted(port);
        produceJsonAndExpectOk(urlValue, "{\"key\": \"my-key\", \"headers\": {\"h1\": \"v1\"}}");
    }

    @Test
    void testSimpleProduceCacheProducer() throws Exception {
        final String topic = genTopic("testSimpleProduceCacheProducer");
        prepareTopicsForTest(topic);
        testGateways =
                new Gateways(
                        List.of(
                                Gateway.builder()
                                        .id("produce")
                                        .type(Gateway.GatewayType.produce)
                                        .topic(topic)
                                        .build(),
                                Gateway.builder()
                                        .id("produce1")
                                        .type(Gateway.GatewayType.produce)
                                        .topic(topic)
                                        .build(),
                                Gateway.builder()
                                        .id("produce2")
                                        .type(Gateway.GatewayType.produce)
                                        .topic(topic)
                                        .build(),
                                Gateway.builder()
                                        .id("consume")
                                        .type(Gateway.GatewayType.consume)
                                        .topic(topic)
                                        .build()));

        final String url =
                "http://localhost:%d/api/gateways/produce/tenant1/application1/produce"
                        .formatted(port);

        produceJsonAndExpectOk(url + "1", "{\"key\": \"my-key\", \"value\": \"my-value\"}");
        produceJsonAndExpectOk(url + "2", "{\"key\": \"my-key\"}");
        produceJsonAndExpectOk(url + "2", "{\"key\": \"my-key\"}");
        produceJsonAndExpectOk(url, "{\"key\": \"my-key\", \"headers\": {\"h1\": \"v1\"}}");

        final String metrics =
                mockMvc.perform(get("/management/prometheus"))
                        .andExpect(status().isOk())
                        .andReturn()
                        .getResponse()
                        .getContentAsString();

        final List<String> cacheMetrics =
                metrics.lines()
                        .filter(l -> l.contains("topic_producer_cache"))
                        .collect(Collectors.toList());
        System.out.println(cacheMetrics);
        assertEquals(5, cacheMetrics.size());

        for (String cacheMetric : cacheMetrics) {
            if (cacheMetric.contains("cache_puts_total")) {
                assertTrue(cacheMetric.contains("3.0"));
            } else if (cacheMetric.contains("hit")) {
                assertTrue(cacheMetric.contains("1.0"));
            } else if (cacheMetric.contains("miss")) {
                assertTrue(cacheMetric.contains("3.0"));
            } else if (cacheMetric.contains("cache_size")) {
                assertTrue(cacheMetric.contains("2.0"));
            } else if (cacheMetric.contains("cache_evictions_total")) {
                assertTrue(cacheMetric.contains("1.0"));
            } else {
                throw new RuntimeException(cacheMetric);
            }
        }
    }

    @Test
    void testParametersRequired() throws Exception {
        final String topic = genTopic("testParametersRequired");
        prepareTopicsForTest(topic);

        testGateways =
                new Gateways(
                        List.of(
                                Gateway.builder()
                                        .id("gw")
                                        .type(Gateway.GatewayType.produce)
                                        .topic(topic)
                                        .parameters(List.of("session-id"))
                                        .build()));

        final String baseUrl =
                "http://localhost:%d/api/gateways/produce/tenant1/application1/gw".formatted(port);

        final String content = "{\"value\": \"my-value\"}";
        produceJsonAndExpectBadRequest(baseUrl, content, "missing required parameter session-id");
        produceJsonAndExpectBadRequest(
                baseUrl + "?param:otherparam=1", content, "missing required parameter session-id");
        produceJsonAndExpectBadRequest(
                baseUrl + "?param:session-id=", content, "missing required parameter session-id");
        produceJsonAndExpectBadRequest(
                baseUrl + "?param:session-id=ok&param:another-non-declared=y",
                content,
                "unknown parameters: [another-non-declared]");
        produceJsonAndExpectOk(baseUrl + "?param:session-id=1", content);
        produceJsonAndExpectOk(baseUrl + "?param:session-id=string-value", content);
    }

    @Test
    void testAuthentication() throws Exception {
        final String topic = genTopic("testAuthentication");
        prepareTopicsForTest(topic);

        testGateways =
                new Gateways(
                        List.of(
                                Gateway.builder()
                                        .id("produce")
                                        .type(Gateway.GatewayType.produce)
                                        .topic(topic)
                                        .authentication(
                                                new Gateway.Authentication(
                                                        "test-auth",
                                                        Map.of(),
                                                        true,
                                                        Gateway.Authentication.HttpCredentialsSource
                                                                .query))
                                        .produceOptions(
                                                new Gateway.ProduceOptions(
                                                        List.of(
                                                                Gateway.KeyValueComparison
                                                                        .valueFromAuthentication(
                                                                                "header1",
                                                                                "login"))))
                                        .build(),
                                Gateway.builder()
                                        .id("consume")
                                        .type(Gateway.GatewayType.consume)
                                        .topic(topic)
                                        .authentication(
                                                new Gateway.Authentication(
                                                        "test-auth",
                                                        Map.of(),
                                                        true,
                                                        Gateway.Authentication.HttpCredentialsSource
                                                                .query))
                                        .consumeOptions(
                                                new Gateway.ConsumeOptions(
                                                        new Gateway.ConsumeOptionsFilters(
                                                                List.of(
                                                                        Gateway.KeyValueComparison
                                                                                .valueFromAuthentication(
                                                                                        "header1",
                                                                                        "login")))))
                                        .build()));

        final String baseUrl =
                "http://localhost:%d/api/gateways/produce/tenant1/application1/produce"
                        .formatted(port);

        produceJsonAndExpectUnauthorized(baseUrl, "{\"value\": \"my-value\"}");
        produceJsonAndExpectUnauthorized(baseUrl + "?credentials=", "{\"value\": \"my-value\"}");
        produceJsonAndExpectUnauthorized(
                baseUrl + "?credentials=error", "{\"value\": \"my-value\"}");
        produceJsonAndExpectOk(
                baseUrl + "?credentials=test-user-password", "{\"value\": \"my-value\"}");

        produceJsonAndExpectBadRequest(
                baseUrl + "?credentials=test-user-password",
                "{\"value\": \"my-value\"}",
                Map.of("Authorization", "test-user-password"),
                "Authorization header is not allowed for this gateway");
    }

    @Test
    void testAuthenticationHttpHeader() throws Exception {
        final String topic = genTopic("testAuthentication");
        prepareTopicsForTest(topic);

        testGateways =
                new Gateways(
                        List.of(
                                Gateway.builder()
                                        .id("produce")
                                        .type(Gateway.GatewayType.produce)
                                        .topic(topic)
                                        .authentication(
                                                new Gateway.Authentication(
                                                        "test-auth",
                                                        Map.of(),
                                                        true,
                                                        Gateway.Authentication.HttpCredentialsSource
                                                                .header))
                                        .produceOptions(
                                                new Gateway.ProduceOptions(
                                                        List.of(
                                                                Gateway.KeyValueComparison
                                                                        .valueFromAuthentication(
                                                                                "header1",
                                                                                "login"))))
                                        .build(),
                                Gateway.builder()
                                        .id("consume")
                                        .type(Gateway.GatewayType.consume)
                                        .topic(topic)
                                        .authentication(
                                                new Gateway.Authentication(
                                                        "test-auth",
                                                        Map.of(),
                                                        true,
                                                        Gateway.Authentication.HttpCredentialsSource
                                                                .header))
                                        .consumeOptions(
                                                new Gateway.ConsumeOptions(
                                                        new Gateway.ConsumeOptionsFilters(
                                                                List.of(
                                                                        Gateway.KeyValueComparison
                                                                                .valueFromAuthentication(
                                                                                        "header1",
                                                                                        "login")))))
                                        .build()));

        final String baseUrl =
                "http://localhost:%d/api/gateways/produce/tenant1/application1/produce"
                        .formatted(port);
        produceJsonAndExpectUnauthorized(baseUrl, "{\"value\": \"my-value\"}", Map.of());
        produceJsonAndExpectUnauthorized(
                baseUrl, "{\"value\": \"my-value\"}", Map.of("Authorization", ""));
        produceJsonAndExpectUnauthorized(
                baseUrl, "{\"value\": \"my-value\"}", Map.of("Authorization", "error"));
        produceJsonAndExpectOk(
                baseUrl,
                "{\"value\": \"my-value\"}",
                Map.of("Authorization", "test-user-password"));
        produceJsonAndExpectOk(
                baseUrl,
                "{\"value\": \"my-value\"}",
                Map.of("authorization", "test-user-password"));

        produceJsonAndExpectBadRequest(
                baseUrl + "?credentials=test-user-password",
                "{\"value\": \"my-value\"}",
                Map.of("Authorization", "test-user-password"),
                "credentials must be passed in the HTTP 'Authorization' header for this gateway");
    }

    @Test
    void testTestCredentials() throws Exception {
        wireMock.register(
                WireMock.get("/auth/tenant1")
                        .withHeader("Authorization", WireMock.equalTo("Bearer test-user-password"))
                        .withHeader("h1", WireMock.equalTo("v1"))
                        .willReturn(WireMock.ok("")));
        final String topic = genTopic("testTestCredentials");
        prepareTopicsForTest(topic);

        testGateways =
                new Gateways(
                        List.of(
                                Gateway.builder()
                                        .id("produce")
                                        .type(Gateway.GatewayType.produce)
                                        .topic(topic)
                                        .authentication(
                                                new Gateway.Authentication(
                                                        "test-auth",
                                                        Map.of(),
                                                        true,
                                                        Gateway.Authentication.HttpCredentialsSource
                                                                .query))
                                        .produceOptions(
                                                new Gateway.ProduceOptions(
                                                        List.of(
                                                                Gateway.KeyValueComparison
                                                                        .valueFromAuthentication(
                                                                                "header1",
                                                                                "login"))))
                                        .build(),
                                Gateway.builder()
                                        .id("produce-no-test")
                                        .type(Gateway.GatewayType.produce)
                                        .topic(topic)
                                        .authentication(
                                                new Gateway.Authentication(
                                                        "test-auth",
                                                        Map.of(),
                                                        false,
                                                        Gateway.Authentication.HttpCredentialsSource
                                                                .query))
                                        .build()));

        final String baseUrl =
                "http://localhost:%d/api/gateways/produce/tenant1/application1/produce"
                        .formatted(port);

        produceJsonAndExpectBadRequest(
                baseUrl + "?test-credentials=test",
                "{\"value\": \"my-value\"}",
                Map.of("X-LangStream-Test-Authorization", "test"),
                "X-LangStream-Test-Authorization header is not allowed for this gateway");
        produceJsonAndExpectUnauthorized(
                baseUrl + "?test-credentials=test", "{\"value\": \"my-value\"}");
        produceJsonAndExpectOk(
                baseUrl + "?test-credentials=test-user-password", "{\"value\": \"my-value\"}");
        produceJsonAndExpectUnauthorized(
                ("http://localhost:%d/api/gateways/produce/tenant1/application1/produce-no-test?test-credentials=test"
                                + "-user-password")
                        .formatted(port),
                "{\"value\": \"my-value\"}");
    }

    @Test
    void testTestCredentialHttpHeader() throws Exception {
        wireMock.register(
                WireMock.get("/auth/tenant1")
                        .withHeader("Authorization", WireMock.equalTo("Bearer test-user-password"))
                        .withHeader("h1", WireMock.equalTo("v1"))
                        .willReturn(WireMock.ok("")));
        final String topic = genTopic("testTestCredentials");
        prepareTopicsForTest(topic);

        testGateways =
                new Gateways(
                        List.of(
                                Gateway.builder()
                                        .id("produce")
                                        .type(Gateway.GatewayType.produce)
                                        .topic(topic)
                                        .authentication(
                                                new Gateway.Authentication(
                                                        "test-auth",
                                                        Map.of(),
                                                        true,
                                                        Gateway.Authentication.HttpCredentialsSource
                                                                .header))
                                        .produceOptions(
                                                new Gateway.ProduceOptions(
                                                        List.of(
                                                                Gateway.KeyValueComparison
                                                                        .valueFromAuthentication(
                                                                                "header1",
                                                                                "login"))))
                                        .build(),
                                Gateway.builder()
                                        .id("produce-no-test")
                                        .type(Gateway.GatewayType.produce)
                                        .topic(topic)
                                        .authentication(
                                                new Gateway.Authentication(
                                                        "test-auth",
                                                        Map.of(),
                                                        false,
                                                        Gateway.Authentication.HttpCredentialsSource
                                                                .header))
                                        .build()));

        final String baseUrl =
                "http://localhost:%d/api/gateways/produce/tenant1/application1/produce"
                        .formatted(port);

        produceJsonAndExpectBadRequest(
                baseUrl + "?test-credentials=test",
                "{\"value\": \"my-value\"}",
                Map.of("X-LangStream-Test-Authorization", "test"),
                "test-credentials must be passed in the HTTP 'X-LangStream-Test-Authorization' header for this gateway");

        produceJsonAndExpectUnauthorized(
                baseUrl,
                "{\"value\": \"my-value\"}",
                Map.of("X-LangStream-Test-Authorization", "test"));
        produceJsonAndExpectOk(
                baseUrl,
                "{\"value\": \"my-value\"}",
                Map.of("X-LangStream-Test-Authorization", "test-user-password"));
    }

    @Test
    void testService() throws Exception {
        final String inputTopic = genTopic("testService-input");
        final String outputTopic = genTopic("testService-output");
        prepareTopicsForTest(inputTopic, outputTopic);

        startTopicExchange(inputTopic, outputTopic, false);

        testGateways =
                new Gateways(
                        List.of(
                                Gateway.builder()
                                        .id("svc")
                                        .type(Gateway.GatewayType.service)
                                        .serviceOptions(
                                                new Gateway.ServiceOptions(
                                                        null,
                                                        inputTopic,
                                                        outputTopic,
                                                        Gateway.ProducePayloadSchema.full,
                                                        List.of()))
                                        .build(),
                                Gateway.builder()
                                        .id("svc-value")
                                        .type(Gateway.GatewayType.service)
                                        .serviceOptions(
                                                new Gateway.ServiceOptions(
                                                        null,
                                                        inputTopic,
                                                        outputTopic,
                                                        Gateway.ProducePayloadSchema.value,
                                                        List.of()))
                                        .build()));

        final String url =
                "http://localhost:%d/api/gateways/service/tenant1/application1/svc".formatted(port);

        assertMessageContent(
                new MsgRecord("my-key", "my-value", Map.of()),
                produceJsonAndGetBody(url, "{\"key\": \"my-key\", \"value\": \"my-value\"}"));
        assertMessageContent(
                new MsgRecord("my-key2", "my-value", Map.of()),
                produceJsonAndGetBody(url, "{\"key\": \"my-key2\", \"value\": \"my-value\"}"));

        assertMessageContent(
                new MsgRecord(null, "my-text", Map.of()), produceTextAndGetBody(url, "my-text"));
        assertMessageContent(
                new MsgRecord("my-key2", "my-value", Map.of("header1", "value1")),
                produceJsonAndGetBody(
                        url,
                        "{\"key\": \"my-key2\", \"value\": \"my-value\", \"headers\": {\"header1\":\"value1\"}}"));

        assertMessageContent(
                new MsgRecord("my-key2", "{\"test\":\"hello\"}", Map.of("header1", "value1")),
                produceJsonAndGetBody(
                        url,
                        "{\"key\": \"my-key2\", \"value\": {\"test\":\"hello\"}, \"headers\": {\"header1\":\"value1\"}}"));

        final String valueUrl =
                "http://localhost:%d/api/gateways/service/tenant1/application1/svc-value"
                        .formatted(port);
        assertMessageContent(
                new MsgRecord(null, "{\"key\":\"my-key\",\"value\":\"my-value\"}", Map.of()),
                produceJsonAndGetBody(valueUrl, "{\"key\": \"my-key\", \"value\": \"my-value\"}"));


        String metrics = getPrometheusMetrics(port);

        List<ApiGatewayTestUtil.ParsedMetric> metricsList = findMetric("langstream_gateways_http_requests_total", metrics);
        assertEquals(2, metricsList.size());
        for (ApiGatewayTestUtil.ParsedMetric parsedMetric : metricsList) {
            assertEquals("langstream_gateways_http_requests_total", parsedMetric.name());
            assertEquals("tenant1", parsedMetric.labels().get("tenant"));
            assertEquals("application1", parsedMetric.labels().get("application"));
            assertEquals("POST", parsedMetric.labels().get("http_method"));
            assertEquals("200", parsedMetric.labels().get("response_status_code"));
            if (parsedMetric.labels().get("gateway").equals("svc")) {
                assertEquals("5.0", parsedMetric.value());
            } else {
                assertEquals("svc-value", parsedMetric.labels().get("gateway"));
                assertEquals("1.0", parsedMetric.value());
            }
        }
    }

    @Test
    void testServiceWithError() throws Exception {
        final String inputTopic = genTopic("testServiceWithError-input");
        final String outputTopic = genTopic("testServiceWithError-output");
        prepareTopicsForTest(inputTopic, outputTopic);

        startTopicExchange(inputTopic, outputTopic, true);

        testGateways =
                new Gateways(
                        List.of(
                                Gateway.builder()
                                        .id("svc")
                                        .type(Gateway.GatewayType.service)
                                        .serviceOptions(
                                                new Gateway.ServiceOptions(
                                                        null,
                                                        inputTopic,
                                                        outputTopic,
                                                        Gateway.ProducePayloadSchema.full,
                                                        List.of()))
                                        .build(),
                                Gateway.builder()
                                        .id("svc-value")
                                        .type(Gateway.GatewayType.service)
                                        .serviceOptions(
                                                new Gateway.ServiceOptions(
                                                        null,
                                                        inputTopic,
                                                        outputTopic,
                                                        Gateway.ProducePayloadSchema.value,
                                                        List.of()))
                                        .build()));

        final String url =
                "http://localhost:%d/api/gateways/service/tenant1/application1/svc".formatted(port);

        HttpRequest request =
                HttpRequest.newBuilder(URI.create(url))
                        .POST(HttpRequest.BodyPublishers.ofString("my-string"))
                        .build();
        HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(500, response.statusCode());
        assertEquals(
                "{\"type\":\"about:blank\",\"title\":\"Internal Server Error\",\"status\":500,\"detail\":\"the agent failed!\",\"instance\":\"/api/gateways/service/tenant1/application1/svc\"}",
                response.body());

        String metrics = getPrometheusMetrics(port);

        List<ApiGatewayTestUtil.ParsedMetric> metricsList = findMetric("langstream_gateways_http_requests_total", metrics);
        assertEquals(1, metricsList.size());
        ApiGatewayTestUtil.ParsedMetric parsedMetric = metricsList.get(0);
        assertEquals("langstream_gateways_http_requests_total", parsedMetric.name());
        assertEquals("tenant1", parsedMetric.labels().get("tenant"));
        assertEquals("application1", parsedMetric.labels().get("application"));
        assertEquals("POST", parsedMetric.labels().get("http_method"));
        assertEquals("500", parsedMetric.labels().get("response_status_code"));
        assertEquals("svc", parsedMetric.labels().get("gateway"));
        assertEquals("1.0", parsedMetric.value());
    }

    private void startTopicExchange(
            String logicalFromTopic, String logicalToTopic, boolean injectAgentFailure)
            throws Exception {
        CompletableFuture<Void> started = new CompletableFuture<>();
        final CompletableFuture<Void> future =
                CompletableFuture.runAsync(
                        () -> {
                            TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry =
                                    topicConnectionsRuntimeProvider
                                            .getTopicConnectionsRuntimeRegistry();
                            final StreamingCluster streamingCluster = getStreamingCluster();
                            final TopicConnectionsRuntime runtime =
                                    topicConnectionsRuntimeRegistry
                                            .getTopicConnectionsRuntime(streamingCluster)
                                            .asTopicConnectionsRuntime();
                            final String fromTopic = resolveTopicName(logicalFromTopic);
                            final String toTopic = resolveTopicName(logicalToTopic);
                            try (final TopicConsumer consumer =
                                    runtime.createConsumer(
                                            "gateway-resource-test" + fromTopic,
                                            streamingCluster,
                                            Map.of(
                                                    "topic",
                                                    fromTopic,
                                                    "subscriptionName",
                                                    "s")); ) {
                                consumer.start();

                                try (final TopicProducer producer =
                                        runtime.createProducer(
                                                "gateway-resource-test" + toTopic,
                                                streamingCluster,
                                                Map.of("topic", toTopic)); ) {

                                    producer.start();
                                    started.complete(null);
                                    while (true) {
                                        if (Thread.currentThread().isInterrupted()) {
                                            break;
                                        }
                                        final List<Record> records = consumer.read();
                                        if (records.isEmpty()) {
                                            continue;
                                        }
                                        log.info(
                                                "read {} records from {}: {}",
                                                records.size(),
                                                fromTopic,
                                                records);
                                        for (Record record : records) {
                                            log.info(
                                                    "read record key {} value {} ({})",
                                                    record.key(),
                                                    record.value(),
                                                    record.value() == null
                                                            ? "NULL"
                                                            : record.value().getClass());
                                            Collection<Header> headers =
                                                    new ArrayList<>(record.headers());
                                            if (injectAgentFailure) {
                                                headers.add(
                                                        SimpleRecord.SimpleHeader.of(
                                                                "langstream-error-message",
                                                                "the agent failed!"));
                                                headers.add(
                                                        SimpleRecord.SimpleHeader.of(
                                                                "langstream-error-type",
                                                                "INTERNAL_ERROR"));
                                            }
                                            producer.write(
                                                            SimpleRecord.copyFrom(record)
                                                                    .headers(headers)
                                                                    .build())
                                                    .get();
                                        }
                                        consumer.commit(records);
                                        log.info(
                                                "written {} records to {}: {}",
                                                records.size(),
                                                toTopic,
                                                records);
                                    }
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            } catch (Throwable e) {
                                if (e.getCause() != null
                                        && e.getCause() instanceof InterruptedException) {
                                    Thread.currentThread().interrupt();
                                } else {
                                    log.error("Error in topic exchange", e);
                                    throw new RuntimeException(e);
                                }
                            } finally {
                                runtime.close();
                            }
                        },
                        futuresExecutor);
        started.get();
        log.info("Topic exchange started");
    }

    private record MsgRecord(Object key, Object value, Map<String, String> headers) {}

    @SneakyThrows
    private void assertMessageContent(MsgRecord expected, String actual) {
        ConsumePushMessage consume = MAPPER.readValue(actual, ConsumePushMessage.class);
        final Map<String, String> headers = consume.record().headers();
        assertNotNull(headers.remove("langstream-service-request-id"));
        final MsgRecord actualMsgRecord =
                new MsgRecord(consume.record().key(), consume.record().value(), headers);

        System.out.println("type: " + actualMsgRecord.value().getClass());

        assertEquals(expected.value(), actualMsgRecord.value());
        assertEquals(expected, actualMsgRecord);
    }

    protected abstract StreamingCluster getStreamingCluster();

    record TopicWithSchema(String topic, String schemaType, String schemaDef) {}

    private void prepareTopicsForTest(String... topic) throws Exception {
        prepareTopicsForTest(
                List.of(topic).stream()
                        .map(
                                t -> {
                                    return new TopicWithSchema(t, null, null);
                                })
                        .toArray(TopicWithSchema[]::new));
    }

    private void prepareTopicsForTest(TopicWithSchema... topic) throws Exception {
        topics = List.of(topic);
        TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry =
                topicConnectionsRuntimeProvider.getTopicConnectionsRuntimeRegistry();
        final ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .pluginsRegistry(new PluginsRegistry())
                        .registry(new ClusterRuntimeRegistry())
                        .topicConnectionsRuntimeRegistry(topicConnectionsRuntimeRegistry)
                        .deployContext(DeployContext.NO_DEPLOY_CONTEXT)
                        .build();
        final StreamingCluster streamingCluster = getStreamingCluster();
        try (TopicConnectionsRuntime runtime =
                topicConnectionsRuntimeRegistry
                        .getTopicConnectionsRuntime(streamingCluster)
                        .asTopicConnectionsRuntime(); ) {
            runtime.deploy(
                    deployer.createImplementation(
                            "app", store.get("t", "app", false).getInstance()));
        }
    }

    protected String resolveTopicName(String topic) {
        return topic;
    }
}
