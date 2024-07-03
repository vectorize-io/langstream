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
package ai.langstream.cli.commands.applications;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GatewaysCmdTest extends CommandTestBase {

    @Test
    public void testProduceHttpQuery() throws Exception {
        Map<String, Object> response =
                Map.of(
                        "application",
                        Map.of(
                                "" + "gateways",
                                Map.of(
                                        "gateways",
                                        List.of(
                                                Map.of(
                                                        "id", "g1",
                                                        "type", "produce",
                                                        "topic", "t1",
                                                        "authentication",
                                                                Map.of(
                                                                        "provider", "basic",
                                                                        "http-credentials-source",
                                                                                "query"))))));

        wireMock.register(
                WireMock.get(String.format("/api/applications/%s/my-app?stats=false", TENANT))
                        .willReturn(WireMock.ok(new ObjectMapper().writeValueAsString(response))));

        wireMock.register(
                WireMock.post(
                                String.format(
                                        "/api/gateways/produce/%s/my-app/g1?credentials=myc",
                                        TENANT))
                        .withRequestBody(
                                equalToJson("{\"key\":null,\"value\":\"hello\",\"headers\":null}"))
                        .willReturn(WireMock.ok()));

        CommandResult result =
                executeCommand(
                        "gateway",
                        "produce",
                        "my-app",
                        "g1",
                        "-c",
                        "myc",
                        "--protocol",
                        "http",
                        "-v",
                        "hello");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
    }

    @Test
    public void testProduceHttpHeader() throws Exception {
        Map<String, Object> response =
                Map.of(
                        "application",
                        Map.of(
                                "" + "gateways",
                                Map.of(
                                        "gateways",
                                        List.of(
                                                Map.of(
                                                        "id", "g1",
                                                        "type", "produce",
                                                        "topic", "t1",
                                                        "authentication",
                                                                Map.of(
                                                                        "provider", "basic",
                                                                        "http-credentials-source",
                                                                                "header"))))));

        wireMock.register(
                WireMock.get(String.format("/api/applications/%s/my-app?stats=false", TENANT))
                        .willReturn(WireMock.ok(new ObjectMapper().writeValueAsString(response))));

        wireMock.register(
                WireMock.post(String.format("/api/gateways/produce/%s/my-app/g1", TENANT))
                        .withHeader("Authorization", equalTo("myc"))
                        .withRequestBody(
                                equalToJson("{\"key\":null,\"value\":\"hello\",\"headers\":null}"))
                        .willReturn(WireMock.ok()));

        CommandResult result =
                executeCommand(
                        "gateway",
                        "produce",
                        "my-app",
                        "g1",
                        "-c",
                        "myc",
                        "--protocol",
                        "http",
                        "-v",
                        "hello");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
    }
}
