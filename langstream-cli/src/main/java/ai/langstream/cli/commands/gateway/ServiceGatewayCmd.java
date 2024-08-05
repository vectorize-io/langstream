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
package ai.langstream.cli.commands.gateway;

import ai.langstream.cli.api.model.Gateways;
import ai.langstream.cli.websocket.WebSocketClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.websocket.CloseReason;
import lombok.SneakyThrows;
import picocli.CommandLine;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@CommandLine.Command(
        name = "service",
        header = "Interact with a service gateway")
public class ServiceGatewayCmd extends BaseGatewayCmd {

    @CommandLine.Parameters(description = "Application ID")
    private String applicationId;

    @CommandLine.Parameters(description = "Gateway ID")
    private String gatewayId;

    @CommandLine.Option(
            names = {"-p", "--param"},
            description = "Gateway parameters. Format: key=value")
    private Map<String, String> params;

    @CommandLine.Option(
            names = {"-c", "--credentials"},
            description =
                    "Credentials for the gateway. Required if the gateway requires authentication.")
    private String credentials;

    @CommandLine.Option(
            names = {"-tc", "--test-credentials"},
            description = "Test credentials for the gateway.")
    private String testCredentials;

    @CommandLine.Option(
            names = {"--connect-timeout"},
            description = "Connect timeout in seconds.")
    private long connectTimeoutSeconds = 0;


    @CommandLine.Option(
            names = {"-v", "--value"},
            description = "Message value")
    private String messageValue;

    @CommandLine.Option(
            names = {"-k", "--key"},
            description = "Message key")
    private String messageKey;

    @CommandLine.Option(
            names = {"--header"},
            description = "Messages headers. Format: key=value")
    private Map<String, String> headers;

    @Override
    @SneakyThrows
    public void run() {
        GatewayRequestInfo gatewayRequestInfo =
                validateGatewayAndGetUrl(
                        applicationId,
                        gatewayId,
                        Gateways.Gateway.TYPE_SERVICE,
                        params,
                        Map.of(),
                        credentials,
                        testCredentials,
                        Protocols.http);
        final Duration connectTimeout =
                connectTimeoutSeconds > 0 ? Duration.ofSeconds(connectTimeoutSeconds) : null;

        String json;
        if (gatewayRequestInfo.isFullPayloadSchema()) {
            final ProduceGatewayCmd.ProduceRequest produceRequest =
                    new ProduceGatewayCmd.ProduceRequest(messageKey, messageValue, headers);
            json = messageMapper.writeValueAsString(produceRequest);
        } else {
            if (messageKey != null) {
                log("Warning: key is ignored when the payload schema is value");
            }
            if (headers != null && !headers.isEmpty()) {
                log("Warning: headers are ignored when the payload schema is value");
            }
            try {
                // it's already a json string
                messageMapper.readValue(messageValue, Map.class);
                json = messageValue;
            } catch (JsonProcessingException ex) {
                json = messageMapper.writeValueAsString(messageValue);
            }

        }

        produceHttp(
                gatewayRequestInfo.getUrl(),
                connectTimeout,
                gatewayRequestInfo.getHeaders(),
                json);

    }

    private void produceHttp(
            String producePath, Duration connectTimeout, Map<String, String> headers, String json)
            throws Exception {
        final HttpRequest.Builder builder =
                HttpRequest.newBuilder(URI.create(producePath))
                        .header("Content-Type", "application/json")
                        .version(HttpClient.Version.HTTP_1_1)
                        .POST(HttpRequest.BodyPublishers.ofString(json));
        if (connectTimeout != null) {
            builder.timeout(connectTimeout);
        }
        if (headers != null) {
            headers.forEach(builder::header);
        }
        final HttpRequest request = builder.build();
        final HttpResponse<String> response =
                getClient()
                        .getHttpClientFacade()
                        .http(request, HttpResponse.BodyHandlers.ofString());
        log("Response: " + response.statusCode());
        log(response.body());
    }
}
