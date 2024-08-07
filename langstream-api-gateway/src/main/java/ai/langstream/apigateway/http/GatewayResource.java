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

import ai.langstream.api.gateway.GatewayRequestContext;
import ai.langstream.api.model.Gateway;
import ai.langstream.api.runner.code.ErrorTypes;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SystemHeaders;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.apigateway.api.ConsumePushMessage;
import ai.langstream.apigateway.api.ProducePayload;
import ai.langstream.apigateway.api.ProduceRequest;
import ai.langstream.apigateway.api.ProduceResponse;
import ai.langstream.apigateway.gateways.*;
import ai.langstream.apigateway.runner.TopicConnectionsRuntimeProviderBean;
import ai.langstream.apigateway.websocket.AuthenticatedGatewayRequestContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.constraints.NotBlank;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.util.UriComponentsBuilder;

@RestController
@RequestMapping("/api/gateways")
@Slf4j
@AllArgsConstructor
public class GatewayResource {

    private static final List<String> RECORD_ERROR_HEADERS =
            List.of(
                    SystemHeaders.ERROR_HANDLING_ERROR_MESSAGE.getKey(),
                    SystemHeaders.ERROR_HANDLING_CAUSE_ERROR_MESSAGE.getKey(),
                    SystemHeaders.ERROR_HANDLING_ROOT_CAUSE_ERROR_MESSAGE.getKey());
    protected static final String GATEWAY_SERVICE_PATH =
            "/service/{tenant}/{application}/{gateway}/**";
    protected static final String SERVICE_REQUEST_ID_HEADER =
            SystemHeaders.SERVICE_REQUEST_ID_HEADER.getKey();
    protected static final ObjectMapper mapper = new ObjectMapper();
    private final TopicConnectionsRuntimeProviderBean topicConnectionsRuntimeRegistryProvider;
    private final ClusterRuntimeRegistry clusterRuntimeRegistry;
    private final TopicProducerCache topicProducerCache;
    private final TopicConnectionsRuntimeCache topicConnectionsRuntimeCache;
    private final ApplicationStore applicationStore;
    private final GatewayRequestHandler gatewayRequestHandler;
    private final ExecutorService httpClientThreadPool =
            Executors.newCachedThreadPool(
                    new BasicThreadFactory.Builder().namingPattern("http-client-%d").build());
    private final HttpClient httpClient =
            HttpClient.newBuilder().executor(httpClientThreadPool).build();
    private final ExecutorService consumeThreadPool =
            Executors.newCachedThreadPool(
                    new BasicThreadFactory.Builder().namingPattern("http-consume-%d").build());

    @PostMapping(value = "/produce/{tenant}/{application}/{gateway}", consumes = "*/*")
    ProduceResponse produce(
            WebRequest request,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("application") String application,
            @NotBlank @PathVariable("gateway") String gateway,
            @RequestBody String payload)
            throws ProduceGateway.ProduceException {

        final Map<String, String> queryString = computeQueryString(request);
        final Map<String, String> headers = computeHeaders(request);
        final GatewayRequestContext context =
                gatewayRequestHandler.validateHttpRequest(
                        tenant,
                        application,
                        gateway,
                        Gateway.GatewayType.produce,
                        queryString,
                        headers,
                        new ProduceGateway.ProduceGatewayRequestValidator());
        final AuthenticatedGatewayRequestContext authContext;
        try {
            authContext = gatewayRequestHandler.authenticate(context);
        } catch (GatewayRequestHandler.AuthFailedException e) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, e.getMessage());
        }

        try (final ProduceGateway produceGateway =
                new ProduceGateway(
                        topicConnectionsRuntimeRegistryProvider
                                .getTopicConnectionsRuntimeRegistry(),
                        clusterRuntimeRegistry,
                        topicProducerCache,
                        topicConnectionsRuntimeCache)) {
            final List<Header> commonHeaders =
                    ProduceGateway.getProducerCommonHeaders(
                            context.gateway().getProduceOptions(), authContext);
            produceGateway.start(context.gateway().getTopic(), commonHeaders, authContext);

            Gateway.ProducePayloadSchema producePayloadSchema =
                    context.gateway().getProduceOptions() == null
                            ? Gateway.ProducePayloadSchema.full
                            : context.gateway().getProduceOptions().payloadSchema();
            final ProducePayload producePayload =
                    parseProducePayload(request, payload, producePayloadSchema);
            produceGateway.produceMessage(producePayload.toProduceRequest());
            return ProduceResponse.OK;
        }
    }

    private ProducePayload parseProducePayload(
            WebRequest request, String payload, Gateway.ProducePayloadSchema payloadSchema)
            throws ProduceGateway.ProduceException {
        final String contentType = request.getHeader("Content-Type");
        if (contentType == null || contentType.equals(MediaType.TEXT_PLAIN_VALUE)) {
            return new ProduceRequest(null, payload, null);
        } else if (contentType.equals(MediaType.APPLICATION_JSON_VALUE)) {
            return ProduceGateway.parseProduceRequest(payload, payloadSchema);
        } else {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    String.format("Unsupported content type: %s", contentType));
        }
    }

    private Map<String, String> computeHeaders(WebRequest request) {
        final Map<String, String> headers =
                new HashMap<>() {
                    @Override
                    public String get(Object key) {
                        return super.get(key != null ? key.toString().toLowerCase() : key);
                    }

                    @Override
                    public String getOrDefault(Object key, String defaultValue) {
                        return super.getOrDefault(
                                key != null ? key.toString().toLowerCase() : key, defaultValue);
                    }

                    @Override
                    public boolean containsKey(Object key) {
                        return super.containsKey(key != null ? key.toString().toLowerCase() : key);
                    }
                };

        request.getHeaderNames()
                .forEachRemaining(name -> headers.put(name, request.getHeader(name)));
        return Collections.unmodifiableMap(headers);
    }

    @PostMapping(value = GATEWAY_SERVICE_PATH)
    CompletableFuture<ResponseEntity> service(
            WebRequest request,
            HttpServletRequest servletRequest,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("application") String application,
            @NotBlank @PathVariable("gateway") String gateway)
            throws Exception {
        return handleServiceCall(request, servletRequest, tenant, application, gateway);
    }

    @GetMapping(value = GATEWAY_SERVICE_PATH)
    CompletableFuture<ResponseEntity> serviceGet(
            WebRequest request,
            HttpServletRequest servletRequest,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("application") String application,
            @NotBlank @PathVariable("gateway") String gateway)
            throws Exception {
        return handleServiceCall(request, servletRequest, tenant, application, gateway);
    }

    @PutMapping(value = GATEWAY_SERVICE_PATH)
    CompletableFuture<ResponseEntity> servicePut(
            WebRequest request,
            HttpServletRequest servletRequest,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("application") String application,
            @NotBlank @PathVariable("gateway") String gateway)
            throws Exception {
        return handleServiceCall(request, servletRequest, tenant, application, gateway);
    }

    @DeleteMapping(value = GATEWAY_SERVICE_PATH)
    CompletableFuture<ResponseEntity> serviceDelete(
            WebRequest request,
            HttpServletRequest servletRequest,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("application") String application,
            @NotBlank @PathVariable("gateway") String gateway)
            throws Exception {
        return handleServiceCall(request, servletRequest, tenant, application, gateway);
    }

    private CompletableFuture<ResponseEntity> handleServiceCall(
            WebRequest request,
            HttpServletRequest servletRequest,
            String tenant,
            String application,
            String gateway)
            throws IOException, ProduceGateway.ProduceException {
        final Map<String, String> queryString = computeQueryString(request);
        final Map<String, String> headers = computeHeaders(request);
        final GatewayRequestContext context =
                gatewayRequestHandler.validateHttpRequest(
                        tenant,
                        application,
                        gateway,
                        Gateway.GatewayType.service,
                        queryString,
                        headers,
                        new GatewayRequestHandler.GatewayRequestValidator() {
                            @Override
                            public List<String> getAllRequiredParameters(Gateway gateway) {
                                return List.of();
                            }

                            @Override
                            public void validateOptions(Map<String, String> options) {}
                        });
        final AuthenticatedGatewayRequestContext authContext;
        try {
            authContext = gatewayRequestHandler.authenticate(context);
        } catch (GatewayRequestHandler.AuthFailedException e) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, e.getMessage());
        }
        if (context.gateway().getServiceOptions().getAgentId() != null) {
            final String uri =
                    applicationStore.getExecutorServiceURI(
                            context.tenant(),
                            context.applicationId(),
                            context.gateway().getServiceOptions().getAgentId());
            return forwardTo(uri, servletRequest.getMethod(), servletRequest);
        } else {
            if (!servletRequest.getMethod().equalsIgnoreCase("post")) {
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST, "Only POST method is supported");
            }
            final String payload =
                    new String(
                            servletRequest.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

            Gateway.ProducePayloadSchema producePayloadSchema =
                    context.gateway().getServiceOptions() == null
                            ? Gateway.ProducePayloadSchema.full
                            : context.gateway().getServiceOptions().getPayloadSchema();
            final ProducePayload producePayload =
                    parseProducePayload(request, payload, producePayloadSchema);
            return handleServiceWithTopics(producePayload, authContext);
        }
    }

    private CompletableFuture<ResponseEntity> handleServiceWithTopics(
            ProducePayload producePayload, AuthenticatedGatewayRequestContext authContext) {

        final String langstreamServiceRequestId = UUID.randomUUID().toString();

        final CompletableFuture<ResponseEntity> completableFuture = new CompletableFuture<>();
        try (final ProduceGateway produceGateway =
                new ProduceGateway(
                        topicConnectionsRuntimeRegistryProvider
                                .getTopicConnectionsRuntimeRegistry(),
                        clusterRuntimeRegistry,
                        topicProducerCache,
                        topicConnectionsRuntimeCache); ) {

            final ConsumeGateway consumeGateway =
                    new ConsumeGateway(
                            topicConnectionsRuntimeRegistryProvider
                                    .getTopicConnectionsRuntimeRegistry(),
                            clusterRuntimeRegistry,
                            topicConnectionsRuntimeCache);
            completableFuture.whenComplete(
                    (r, t) -> {
                        consumeGateway.close();
                    });

            final Gateway.ServiceOptions serviceOptions = authContext.gateway().getServiceOptions();
            try {
                final List<Function<Record, Boolean>> messageFilters =
                        ConsumeGateway.createMessageFilters(
                                serviceOptions.getHeaders(),
                                authContext.userParameters(),
                                authContext.principalValues());
                messageFilters.add(
                        record -> {
                            final Header header = record.getHeader(SERVICE_REQUEST_ID_HEADER);
                            if (header == null) {
                                return false;
                            }
                            return langstreamServiceRequestId.equals(header.valueAsString());
                        });
                consumeGateway.setup(serviceOptions.getOutputTopic(), messageFilters, authContext);
                final AtomicBoolean stop = new AtomicBoolean(false);
                consumeGateway.startReadingAsync(
                        consumeThreadPool,
                        stop::get,
                        record -> {
                            stop.set(true);
                            completableFuture.complete(buildResponseFromReceivedRecord(record));
                        },
                        completableFuture::completeExceptionally);
            } catch (Exception ex) {
                log.error("Error while setting up consume gateway", ex);
                throw new RuntimeException(ex);
            }
            final List<Header> commonHeaders =
                    ProduceGateway.getProducerCommonHeaders(serviceOptions, authContext);
            produceGateway.start(serviceOptions.getInputTopic(), commonHeaders, authContext);

            Map<String, String> passedHeaders = producePayload.headers();
            if (passedHeaders == null) {
                passedHeaders = new HashMap<>();
            }
            passedHeaders.put(SERVICE_REQUEST_ID_HEADER, langstreamServiceRequestId);
            produceGateway.produceMessage(
                    new ProduceRequest(
                            producePayload.key(), producePayload.value(), passedHeaders));
        } catch (Throwable t) {
            log.error("Error on service gateway", t);
            completableFuture.completeExceptionally(t);
        }
        return completableFuture;
    }

    private static ResponseEntity<Object> buildResponseFromReceivedRecord(
            ConsumePushMessage consumePushMessage) {
        Objects.requireNonNull(consumePushMessage);
        ConsumePushMessage.Record record = consumePushMessage.record();
        if (record != null && record.headers() != null) {
            String errorType =
                    record.headers().get(SystemHeaders.ERROR_HANDLING_ERROR_TYPE.getKey());
            if (errorType != null) {
                int statusCode = convertRecordErrorToStatusCode(errorType);
                String errorMessage = convertRecordErrorToHttpResponseMessage(record);
                return ResponseEntity.status(statusCode)
                        .body(
                                ProblemDetail.forStatusAndDetail(
                                        HttpStatus.valueOf(statusCode), errorMessage));
            }
        }
        try {
            String asString = mapper.writeValueAsString(consumePushMessage);
            return ResponseEntity.ok(asString);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String convertRecordErrorToHttpResponseMessage(
            ConsumePushMessage.Record record) {
        String errorMessage = null;
        for (String header : RECORD_ERROR_HEADERS) {
            String value = record.headers().get(header);
            if (!StringUtils.isEmpty(value)) {
                errorMessage = value;
                break;
            }
        }
        if (errorMessage == null) {
            if (record.value() != null) {
                errorMessage = record.value().toString();
            } else {
                // in this case the user will only have the status code as a hint
                errorMessage = "";
            }
        }
        return errorMessage;
    }

    private static int convertRecordErrorToStatusCode(String errorTypeString) {
        ErrorTypes errorType = ErrorTypes.valueOf(errorTypeString.toUpperCase());
        return switch (errorType) {
            case INVALID_RECORD -> 400;
            case INTERNAL_ERROR -> 500;
        };
    }

    private Map<String, String> computeQueryString(WebRequest request) {
        final Map<String, String> queryString =
                request.getParameterMap().keySet().stream()
                        .collect(Collectors.toMap(k -> k, k -> request.getParameter(k)));
        return queryString;
    }

    private CompletableFuture<ResponseEntity> forwardTo(
            String agentURI, String method, HttpServletRequest request) {
        try {
            String requestUrl = request.getRequestURI();
            final String[] parts = requestUrl.split("/", 8);
            final List<String> partsList =
                    Arrays.stream(parts).filter(s -> !s.isBlank()).collect(Collectors.toList());

            // /api/gateways/service/tenant/application/gateway/<part>
            if (partsList.size() > 6) {
                requestUrl = "/" + partsList.get(6);
            } else {
                requestUrl = "/";
            }
            final URI uri =
                    UriComponentsBuilder.fromUri(URI.create(agentURI))
                            .path(requestUrl)
                            .query(request.getQueryString())
                            .build(true)
                            .toUri();
            log.debug("Forwarding service request to {}, method {}", uri, method);

            final HttpRequest.Builder requestBuilder =
                    HttpRequest.newBuilder(uri)
                            .version(HttpClient.Version.HTTP_1_1)
                            .method(
                                    method,
                                    HttpRequest.BodyPublishers.ofInputStream(
                                            () -> {
                                                try {
                                                    return request.getInputStream();
                                                } catch (IOException e) {
                                                    throw new RuntimeException(e);
                                                }
                                            }));

            request.getHeaderNames()
                    .asIterator()
                    .forEachRemaining(
                            h -> {
                                switch (h) {
                                        // from jdk.internal.net.http.common.Utils
                                    case "connection":
                                    case "content-length":
                                    case "expect":
                                    case "host":
                                    case "upgrade":
                                        return;
                                    default:
                                        requestBuilder.header(h, request.getHeader(h));
                                        break;
                                }
                            });

            final HttpRequest httpRequest = requestBuilder.build();
            return httpClient
                    .sendAsync(httpRequest, HttpResponse.BodyHandlers.ofInputStream())
                    .thenApply(
                            remoteResponse -> {
                                final ResponseEntity.BodyBuilder responseBuilder =
                                        ResponseEntity.status(remoteResponse.statusCode());
                                remoteResponse
                                        .headers()
                                        .map()
                                        .forEach(
                                                (k, v) -> {
                                                    responseBuilder.header(k, v.get(0));
                                                });

                                return responseBuilder.body(
                                        new InputStreamResource(remoteResponse.body()));
                            });
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    @PreDestroy
    public void onDestroy() throws Exception {
        log.info("Shutting down GatewayResource");
        httpClientThreadPool.shutdownNow();
        consumeThreadPool.shutdownNow();
        consumeThreadPool.awaitTermination(1, TimeUnit.MINUTES);
        httpClientThreadPool.awaitTermination(1, TimeUnit.MINUTES);
    }
}
