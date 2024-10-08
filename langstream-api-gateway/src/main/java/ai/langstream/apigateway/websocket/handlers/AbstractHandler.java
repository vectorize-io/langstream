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
package ai.langstream.apigateway.websocket.handlers;

import ai.langstream.api.events.EventRecord;
import ai.langstream.api.events.EventSources;
import ai.langstream.api.events.GatewayEventData;
import ai.langstream.api.model.Gateway;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.StreamingClusterRuntime;
import ai.langstream.api.runtime.Topic;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.api.util.ObjectMapperFactory;
import ai.langstream.apigateway.api.ProduceResponse;
import ai.langstream.apigateway.gateways.*;
import ai.langstream.apigateway.util.StreamingClusterUtil;
import ai.langstream.apigateway.websocket.AuthenticatedGatewayRequestContext;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@Slf4j
public abstract class AbstractHandler extends TextWebSocketHandler {
    protected static final String ATTRIBUTE_PRODUCE_GATEWAY = "__produce_gateway";
    protected static final String ATTRIBUTE_CONSUME_GATEWAY = "__consume_gateway";
    protected final TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry;
    protected final ClusterRuntimeRegistry clusterRuntimeRegistry;
    protected final ApplicationStore applicationStore;
    private final TopicProducerCache topicProducerCache;

    protected final TopicConnectionsRuntimeCache topicConnectionsRuntimeCache;

    public AbstractHandler(
            ApplicationStore applicationStore,
            TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry,
            ClusterRuntimeRegistry clusterRuntimeRegistry,
            TopicProducerCache topicProducerCache,
            TopicConnectionsRuntimeCache topicConnectionsRuntimeCache) {
        this.topicConnectionsRuntimeRegistry = topicConnectionsRuntimeRegistry;
        this.clusterRuntimeRegistry = clusterRuntimeRegistry;
        this.applicationStore = applicationStore;
        this.topicProducerCache = topicProducerCache;
        this.topicConnectionsRuntimeCache = topicConnectionsRuntimeCache;
    }

    public abstract String path();

    public abstract Gateway.GatewayType gatewayType();

    public abstract String tenantFromPath(
            Map<String, String> parsedPath, Map<String, String> queryString);

    public abstract String applicationIdFromPath(
            Map<String, String> parsedPath, Map<String, String> queryString);

    public abstract String gatewayFromPath(
            Map<String, String> parsedPath, Map<String, String> queryString);

    public abstract GatewayRequestHandler.GatewayRequestValidator validator();

    public void onBeforeHandshakeCompleted(
            AuthenticatedGatewayRequestContext gatewayRequestContext,
            Map<String, Object> attributes)
            throws Exception {}

    abstract void onOpen(
            WebSocketSession webSocketSession,
            AuthenticatedGatewayRequestContext gatewayRequestContext)
            throws Exception;

    abstract void onMessage(
            WebSocketSession webSocketSession,
            AuthenticatedGatewayRequestContext gatewayRequestContext,
            TextMessage message)
            throws Exception;

    abstract void onClose(
            WebSocketSession webSocketSession,
            AuthenticatedGatewayRequestContext gatewayRequestContext,
            CloseStatus status)
            throws Exception;

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        super.afterConnectionEstablished(session);
        try {
            onOpen(session, getContext(session));
        } catch (Throwable throwable) {
            log.error("[{}] error while opening websocket", session.getId(), throwable);
            closeSession(session, throwable);
        }
    }

    private AuthenticatedGatewayRequestContext getContext(WebSocketSession session) {
        return (AuthenticatedGatewayRequestContext) session.getAttributes().get("context");
    }

    private void closeSession(WebSocketSession session, Throwable throwable) {
        CloseStatus status = CloseStatus.SERVER_ERROR;
        if (throwable instanceof IllegalArgumentException) {
            status = CloseStatus.POLICY_VIOLATION;
        }
        try {
            session.close(status.withReason(throwable.getMessage()));
        } catch (IOException e) {
            log.error("error while closing websocket", e);
        } finally {
            callHandlerOnClose(session, status);
        }
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        try {
            onMessage(session, getContext(session), message);
        } catch (Throwable throwable) {
            log.error("[{}] error while opening websocket", session.getId(), throwable);
            closeSession(session, throwable);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status)
            throws Exception {
        super.afterConnectionClosed(session, status);
        callHandlerOnClose(session, status);
        sendClientDisconnectedEvent(getContext(session));
    }

    private void callHandlerOnClose(WebSocketSession session, CloseStatus status) {
        try {
            onClose(session, getContext(session), status);
        } catch (Throwable throwable) {
            log.error("[{}] error while closing websocket", session.getId(), throwable);
        }
    }

    @Override
    public boolean supportsPartialMessages() {
        return true;
    }

    protected void sendClientConnectedEvent(AuthenticatedGatewayRequestContext context) {
        sendEvent(EventRecord.Types.ClientConnected, context);
    }

    protected void sendClientDisconnectedEvent(AuthenticatedGatewayRequestContext context) {
        try {
            sendEvent(EventRecord.Types.ClientDisconnected, context);
        } catch (Throwable e) {
            log.error("error while sending client disconnected event", e);
        }
    }

    @SneakyThrows
    protected void sendEvent(EventRecord.Types type, AuthenticatedGatewayRequestContext context) {
        final Gateway gateway = context.gateway();
        if (gateway.getEventsTopic() == null) {
            return;
        }

        final StreamingCluster streamingCluster =
                context.application().getInstance().streamingCluster();

        TopicConnectionsRuntimeCache.Key key =
                new TopicConnectionsRuntimeCache.Key(
                        context.tenant(),
                        context.applicationId(),
                        context.gateway().getId(),
                        StreamingClusterUtil.asKey(streamingCluster));

        try (final TopicConnectionsRuntime topicConnectionsRuntime =
                topicConnectionsRuntimeCache.getOrCreate(
                        key,
                        () ->
                                topicConnectionsRuntimeRegistry
                                        .getTopicConnectionsRuntime(streamingCluster)
                                        .asTopicConnectionsRuntime())) {

            TopicDefinition topicDefinition =
                    context.application().resolveTopic(gateway.getEventsTopic());
            StreamingClusterRuntime streamingClusterRuntime =
                    clusterRuntimeRegistry.getStreamingClusterRuntime(streamingCluster);
            Topic topicImplementation =
                    streamingClusterRuntime.createTopicImplementation(
                            topicDefinition, streamingCluster);
            final String resolvedTopicName = topicImplementation.topicName();
            final TopicProducerCache.Key topicProducerKey =
                    new TopicProducerCache.Key(
                            context.tenant(),
                            context.applicationId(),
                            context.gateway().getId(),
                            resolvedTopicName,
                            StreamingClusterUtil.asKey(streamingCluster));

            TopicProducer eventsProducer =
                    topicProducerCache.getOrCreate(
                            topicProducerKey,
                            new Supplier<TopicProducer>() {
                                @Override
                                public TopicProducer get() {
                                    TopicProducer prod =
                                            topicConnectionsRuntime.createProducer(
                                                    null,
                                                    streamingCluster,
                                                    Map.of("topic", resolvedTopicName));
                                    prod.start();
                                    return prod;
                                }
                            });
            try (eventsProducer) {
                final EventSources.GatewaySource source =
                        EventSources.GatewaySource.builder()
                                .tenant(context.tenant())
                                .applicationId(context.applicationId())
                                .gateway(gateway)
                                .build();

                final GatewayEventData data =
                        GatewayEventData.builder()
                                .userParameters(context.userParameters())
                                .options(context.options())
                                .httpRequestHeaders(context.httpHeaders())
                                .build();

                final EventRecord event =
                        EventRecord.builder()
                                .category(EventRecord.Categories.Gateway)
                                .type(type.toString())
                                .timestamp(System.currentTimeMillis())
                                .source(
                                        ObjectMapperFactory.getDefaultMapper()
                                                .convertValue(source, Map.class))
                                .data(
                                        ObjectMapperFactory.getDefaultMapper()
                                                .convertValue(data, Map.class))
                                .build();

                final String recordValue =
                        ObjectMapperFactory.getDefaultMapper().writeValueAsString(event);

                final SimpleRecord record = SimpleRecord.builder().value(recordValue).build();
                eventsProducer.write(record).get();
                log.info("sent event {}", recordValue);
            }
        }
    }

    protected void startReadingMessages(WebSocketSession webSocketSession, Executor executor) {
        final AuthenticatedGatewayRequestContext context = getContext(webSocketSession);
        final ConsumeGateway consumeGateway =
                (ConsumeGateway) context.attributes().get(ATTRIBUTE_CONSUME_GATEWAY);
        consumeGateway.startReadingAsync(
                executor,
                () -> !webSocketSession.isOpen(),
                message -> {
                    try {
                        String jsonStringMessage =
                                ObjectMapperFactory.getDefaultMapper().writeValueAsString(message);
                        webSocketSession.sendMessage(new TextMessage(jsonStringMessage));
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                },
                throwable -> {
                    log.error("error while reading messages", throwable);
                    closeSession(webSocketSession, throwable);
                });
    }

    protected void setupReader(
            String topic,
            List<Function<Record, Boolean>> filters,
            AuthenticatedGatewayRequestContext context)
            throws Exception {
        final ConsumeGateway consumeGateway =
                new ConsumeGateway(
                        topicConnectionsRuntimeRegistry,
                        clusterRuntimeRegistry,
                        topicConnectionsRuntimeCache);
        try {
            consumeGateway.setup(topic, filters, context);
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            consumeGateway.close();
            throw ex;
        }
        context.attributes().put(ATTRIBUTE_CONSUME_GATEWAY, consumeGateway);
    }

    protected void setupProducer(
            String topic, List<Header> commonHeaders, AuthenticatedGatewayRequestContext context)
            throws Exception {
        final ProduceGateway produceGateway =
                new ProduceGateway(
                        topicConnectionsRuntimeRegistry,
                        clusterRuntimeRegistry,
                        topicProducerCache,
                        topicConnectionsRuntimeCache);

        try {
            produceGateway.start(topic, commonHeaders, context);
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            produceGateway.close();
            throw ex;
        }
        context.attributes().put(ATTRIBUTE_PRODUCE_GATEWAY, produceGateway);
    }

    protected void produceMessage(
            WebSocketSession webSocketSession,
            TextMessage message,
            Gateway.ProducePayloadSchema payloadSchema)
            throws IOException {
        try {
            final AuthenticatedGatewayRequestContext context = getContext(webSocketSession);
            final ProduceGateway produceGateway =
                    (ProduceGateway) context.attributes().get(ATTRIBUTE_PRODUCE_GATEWAY);
            produceGateway.produceMessage(message.getPayload(), payloadSchema);
            webSocketSession.sendMessage(
                    new TextMessage(
                            ObjectMapperFactory.getDefaultMapper()
                                    .writeValueAsString(ProduceResponse.OK)));
        } catch (ProduceGateway.ProduceException exception) {
            sendResponse(webSocketSession, exception.getStatus(), exception.getMessage());
        }
    }

    protected void closeConsumeGateway(WebSocketSession webSocketSession) {
        closeConsumeGateway(getContext(webSocketSession));
    }

    protected void closeConsumeGateway(AuthenticatedGatewayRequestContext context) {
        final ConsumeGateway consumeGateway =
                (ConsumeGateway) context.attributes().get(ATTRIBUTE_CONSUME_GATEWAY);
        if (consumeGateway == null) {
            return;
        }
        consumeGateway.close();
    }

    protected void closeProduceGateway(WebSocketSession webSocketSession) {
        closeProduceGateway(getContext(webSocketSession));
    }

    protected void closeProduceGateway(AuthenticatedGatewayRequestContext context) {
        final ProduceGateway produceGateway =
                (ProduceGateway) context.attributes().get(ATTRIBUTE_PRODUCE_GATEWAY);
        if (produceGateway == null) {
            return;
        }
        produceGateway.close();
    }

    private static void sendResponse(
            WebSocketSession webSocketSession, ProduceResponse.Status status, String reason)
            throws IOException {
        webSocketSession.sendMessage(
                new TextMessage(
                        ObjectMapperFactory.getDefaultMapper()
                                .writeValueAsString(new ProduceResponse(status, reason))));
    }
}
