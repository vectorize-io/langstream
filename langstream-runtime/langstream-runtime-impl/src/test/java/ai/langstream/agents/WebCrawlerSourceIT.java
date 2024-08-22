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
package ai.langstream.agents;

import static ai.langstream.testrunners.AbstractApplicationRunner.INTEGRATION_TESTS_GROUP2;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.util.ObjectMapperFactory;
import ai.langstream.testrunners.AbstractGenericStreamingApplicationRunner;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Slf4j
@WireMockTest
@Tag(INTEGRATION_TESTS_GROUP2)
class WebCrawlerSourceIT extends AbstractGenericStreamingApplicationRunner {

    static WireMockRuntimeInfo wireMockRuntimeInfo;

    @BeforeAll
    static void onBeforeAll(WireMockRuntimeInfo info) {
        wireMockRuntimeInfo = info;
    }

    @Test
    public void test(WireMockRuntimeInfo vmRuntimeInfo) throws Exception {

        stubFor(
                get("/index.html")
                        .willReturn(
                                okForContentType(
                                        "text/html",
                                        """
                                <a href="secondPage.html">link</a>
                            """)));
        stubFor(
                get("/secondPage.html")
                        .willReturn(
                                okForContentType(
                                        "text/html",
                                        """
                                  <a href="thirdPage.html">link</a>
                                  <a href="index.html">link to home</a>
                              """)));
        stubFor(
                get("/thirdPage.html")
                        .willReturn(
                                okForContentType(
                                        "text/html",
                                        """
                                  Hello!
                              """)));

        final String appId = "app-" + UUID.randomUUID().toString().substring(0, 4);

        String tenant = "tenant";

        String[] expectedAgents = new String[] {appId + "-step1"};

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "${globals.output-topic}"
                                    creation-mode: create-if-not-exists
                                  - name: "deleted-documents"
                                    creation-mode: create-if-not-exists
                                  - name: "activities"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - type: "webcrawler-source"
                                    id: "step1"
                                    output: "${globals.output-topic}"
                                    configuration:\s
                                        seed-urls: ["%s/index.html"]
                                        allow-non-html-contents: true
                                        allowed-domains: ["%s"]
                                        state-storage: disk
                                        max-unflushed-pages: 1
                                        deleted-documents-topic: "deleted-documents"
                                        source-activity-summary-topic: activities
                                        source-activity-summary-events: new,changed,unchanged,deleted
                                        source-activity-summary-events-threshold: 2
                                        source-activity-summary-time-seconds-threshold: 5
                                """
                                .formatted(
                                        wireMockRuntimeInfo.getHttpBaseUrl(),
                                        wireMockRuntimeInfo.getHttpBaseUrl()));
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, appId, application, buildInstanceYaml(), expectedAgents)) {

            try (TopicConsumer deletedDocumentsConsumer = createConsumer("deleted-documents");
                    TopicConsumer activitiesConsumer = createConsumer("activities");
                    TopicConsumer consumer =
                            createConsumer(applicationRuntime.getGlobal("output-topic")); ) {

                executeAgentRunners(applicationRuntime);

                waitForMessages(
                        consumer,
                        List.of(
                                """
                        <html>
                         <head></head>
                         <body>
                          <a href="secondPage.html">link</a>
                         </body>
                        </html>""",
                                """
                                <html>
                                 <head></head>
                                 <body>
                                  <a href="thirdPage.html">link</a> <a href="index.html">link to home</a>
                                 </body>
                                </html>""",
                                """
                                <html>
                                 <head></head>
                                 <body>
                                  Hello!
                                 </body>
                                </html>"""));

                String expected = "%s-%s.webcrawler.status.json".formatted(appId, "step1");
                final Path statusFile =
                        getBasePersistenceDirectory().resolve("step1").resolve(expected);
                assertTrue(Files.exists(statusFile));

                stubFor(get("/thirdPage.html").willReturn(notFound()));

                executeAgentRunners(applicationRuntime);

                waitForMessages(
                        deletedDocumentsConsumer,
                        List.of(
                                "%s/thirdPage.html"
                                        .formatted(wireMockRuntimeInfo.getHttpBaseUrl())));

                waitForMessages(
                        activitiesConsumer,
                        List.of(
                                new java.util.function.Consumer<Object>() {
                                    @Override
                                    @SneakyThrows
                                    public void accept(Object o) {
                                        Map map =
                                                ObjectMapperFactory.getDefaultMapper().readValue((String) o, Map.class);

                                        List<Map<String, Object>> newUrls =
                                                (List<Map<String, Object>>) map.get("newObjects");
                                        List<Map<String, Object>> changed =
                                                (List<Map<String, Object>>)
                                                        map.get("updatedObjects");
                                        List<Map<String, Object>> unchanged =
                                                (List<Map<String, Object>>)
                                                        map.get("unchangedObjects");
                                        List<Map<String, Object>> deleted =
                                                (List<Map<String, Object>>)
                                                        map.get("deletedObjects");
                                        assertTrue(changed.isEmpty());
                                        assertTrue(unchanged.isEmpty());
                                        assertTrue(deleted.isEmpty());
                                        assertEquals(2, newUrls.size());
                                        assertEquals(
                                                "%s/index.html"
                                                        .formatted(
                                                                wireMockRuntimeInfo
                                                                        .getHttpBaseUrl()),
                                                newUrls.get(0).get("object"));
                                        assertNotNull(newUrls.get(0).get("detectedAt"));
                                        assertEquals(
                                                "%s/secondPage.html"
                                                        .formatted(
                                                                wireMockRuntimeInfo
                                                                        .getHttpBaseUrl()),
                                                newUrls.get(1).get("object"));
                                        assertNotNull(newUrls.get(1).get("detectedAt"));
                                    }
                                },
                                new java.util.function.Consumer<Object>() {
                                    @Override
                                    @SneakyThrows
                                    public void accept(Object o) {
                                        Map map =
                                                ObjectMapperFactory.getDefaultMapper().readValue((String) o, Map.class);

                                        List<Map<String, Object>> newUrls =
                                                (List<Map<String, Object>>) map.get("newObjects");
                                        List<Map<String, Object>> changed =
                                                (List<Map<String, Object>>)
                                                        map.get("updatedObjects");
                                        List<Map<String, Object>> unchanged =
                                                (List<Map<String, Object>>)
                                                        map.get("unchangedObjects");
                                        List<Map<String, Object>> deleted =
                                                (List<Map<String, Object>>)
                                                        map.get("deletedObjects");
                                        assertTrue(changed.isEmpty());
                                        assertTrue(unchanged.isEmpty());
                                        assertEquals(1, deleted.size());
                                        assertEquals(1, newUrls.size());
                                        assertEquals(
                                                "%s/thirdPage.html"
                                                        .formatted(
                                                                wireMockRuntimeInfo
                                                                        .getHttpBaseUrl()),
                                                newUrls.get(0).get("object"));
                                        assertNotNull(newUrls.get(0).get("detectedAt"));
                                        assertEquals(
                                                "%s/thirdPage.html"
                                                        .formatted(
                                                                wireMockRuntimeInfo
                                                                        .getHttpBaseUrl()),
                                                deleted.get(0).get("object"));
                                        assertNotNull(deleted.get(0).get("detectedAt"));
                                    }
                                }));
            }
        }
    }

    @Test
    public void testInvalidate(WireMockRuntimeInfo vmRuntimeInfo) throws Exception {

        stubFor(
                get("/index.html")
                        .willReturn(
                                okForContentType(
                                        "text/html",
                                        """
                                <a href="secondPage.html">link</a>
                            """)));
        stubFor(
                get("/secondPage.html")
                        .willReturn(
                                okForContentType(
                                        "text/html",
                                        """
                                  <a href="thirdPage.html">link</a>
                                  <a href="index.html">link to home</a>
                              """)));
        stubFor(
                get("/thirdPage.html")
                        .willReturn(
                                okForContentType(
                                        "text/html",
                                        """
                                  Hello!
                              """)));

        final String appId = "app-" + UUID.randomUUID().toString().substring(0, 4);

        String tenant = "tenant";

        String[] expectedAgents = new String[] {appId + "-step1"};

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "${globals.output-topic}"
                                    creation-mode: create-if-not-exists
                                  - name: "signals"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - type: "webcrawler-source"
                                    id: "step1"
                                    output: "${globals.output-topic}"
                                    signals-from: "signals"
                                    configuration:\s
                                        seed-urls: ["%s/index.html"]
                                        allow-non-html-contents: true
                                        allowed-domains: ["%s"]
                                        state-storage: disk
                                        max-unflushed-pages: 1
                                """
                                .formatted(
                                        wireMockRuntimeInfo.getHttpBaseUrl(),
                                        wireMockRuntimeInfo.getHttpBaseUrl()));
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, appId, application, buildInstanceYaml(), expectedAgents)) {

            try (TopicConsumer consumer =
                            createConsumer(applicationRuntime.getGlobal("output-topic"));
                    TopicProducer producer = createProducer("signals"); ) {

                executeAgentRunners(applicationRuntime);

                waitForMessages(
                        consumer,
                        List.of(
                                """
                        <html>
                         <head></head>
                         <body>
                          <a href="secondPage.html">link</a>
                         </body>
                        </html>""",
                                """
                                <html>
                                 <head></head>
                                 <body>
                                  <a href="thirdPage.html">link</a> <a href="index.html">link to home</a>
                                 </body>
                                </html>""",
                                """
                                <html>
                                 <head></head>
                                 <body>
                                  Hello!
                                 </body>
                                </html>"""));
                sendFullMessage(producer, "invalidate-all", null, List.of());

                executeAgentRunners(applicationRuntime);

                waitForMessages(
                        consumer,
                        List.of(
                                """
                        <html>
                         <head></head>
                         <body>
                          <a href="secondPage.html">link</a>
                         </body>
                        </html>""",
                                """
                                <html>
                                 <head></head>
                                 <body>
                                  <a href="thirdPage.html">link</a> <a href="index.html">link to home</a>
                                 </body>
                                </html>""",
                                """
                                <html>
                                 <head></head>
                                 <body>
                                  Hello!
                                 </body>
                                </html>"""));
            }
        }
    }
}
