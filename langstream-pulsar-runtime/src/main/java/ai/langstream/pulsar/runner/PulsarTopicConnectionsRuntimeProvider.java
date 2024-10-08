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
package ai.langstream.pulsar.runner;

import static ai.langstream.pulsar.PulsarClientUtils.buildPulsarAdmin;
import static ai.langstream.pulsar.PulsarClientUtils.getPulsarClusterRuntimeConfiguration;
import static java.util.Map.entry;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.SchemaDefinition;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.TopicAdmin;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeProvider;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicOffsetPosition;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runner.topics.TopicReadResult;
import ai.langstream.api.runner.topics.TopicReader;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.Topic;
import ai.langstream.api.util.ObjectMapperFactory;
import ai.langstream.pulsar.PulsarClientUtils;
import ai.langstream.pulsar.PulsarClusterRuntimeConfiguration;
import ai.langstream.pulsar.PulsarName;
import ai.langstream.pulsar.PulsarTopic;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

@Slf4j
public class PulsarTopicConnectionsRuntimeProvider implements TopicConnectionsRuntimeProvider {

    @Override
    public boolean supports(String streamingClusterType) {
        return "pulsar".equals(streamingClusterType);
    }

    @Override
    public TopicConnectionsRuntime getImplementation() {
        return new PulsarTopicConnectionsRuntime();
    }

    private static class PulsarTopicConnectionsRuntime implements TopicConnectionsRuntime {

        private volatile PulsarClient client;

        @SneakyThrows
        public synchronized void initClient(StreamingCluster streamingCluster) {
            if (client != null) {
                return;
            }
            client = PulsarClientUtils.buildPulsarClient(streamingCluster);
        }

        @Override
        @SneakyThrows
        public synchronized void close() {
            if (client != null) {
                client.close();
            }
            client = null;
        }

        @Override
        public TopicReader createReader(
                StreamingCluster streamingCluster,
                Map<String, Object> configuration,
                TopicOffsetPosition initialPosition) {
            initClient(streamingCluster);
            Map<String, Object> copy = new HashMap<>(configuration);
            applyFullQualifiedTopicName(streamingCluster, copy);
            return new PulsarTopicReader(copy, initialPosition);
        }

        @Override
        public TopicConsumer createConsumer(
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {
            initClient(streamingCluster);
            Map<String, Object> copy = new HashMap<>(configuration);
            copy.remove("deadLetterTopicProducer");
            applyFullQualifiedTopicName(streamingCluster, copy);
            return new PulsarTopicConsumer(copy);
        }

        @Override
        public TopicProducer createProducer(
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {
            initClient(streamingCluster);
            Map<String, Object> copy = new HashMap<>(configuration);
            applyFullQualifiedTopicName(streamingCluster, copy);
            return new PulsarTopicProducer<>(copy);
        }

        private static void applyFullQualifiedTopicName(
                StreamingCluster streamingCluster, Map<String, Object> copy) {
            String topic = (String) copy.get("topic");
            if (topic != null && !topic.contains("/")) {
                PulsarClusterRuntimeConfiguration runtimeConfiguration =
                        getPulsarClusterRuntimeConfiguration(streamingCluster);
                topic =
                        new PulsarName(
                                        runtimeConfiguration.defaultTenant(),
                                        runtimeConfiguration.defaultNamespace(),
                                        topic)
                                .toPulsarName();
                copy.put("topic", topic);
            }
        }

        @Override
        public TopicProducer createDeadletterTopicProducer(
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {
            initClient(streamingCluster);
            Map<String, Object> deadletterConfiguration =
                    (Map<String, Object>) configuration.get("deadLetterTopicProducer");
            if (deadletterConfiguration == null || deadletterConfiguration.isEmpty()) {
                return null;
            }
            log.info(
                    "Creating deadletter topic producer for agent {} using configuration {}",
                    agentId,
                    configuration);
            return createProducer(agentId, streamingCluster, deadletterConfiguration);
        }

        @Override
        public TopicAdmin createTopicAdmin(
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {
            return new TopicAdmin() {};
        }

        @Override
        @SneakyThrows
        public void deploy(ExecutionPlan applicationInstance) {
            Application logicalInstance = applicationInstance.getApplication();
            PulsarClusterRuntimeConfiguration configuration =
                    getPulsarClusterRuntimeConfiguration(
                            logicalInstance.getInstance().streamingCluster());
            try (PulsarAdmin admin = buildPulsarAdmin(configuration)) {
                applyRetentionPolicies(
                        admin,
                        "%s/%s"
                                .formatted(
                                        configuration.defaultTenant(),
                                        configuration.defaultNamespace()),
                        configuration.defaultRetentionPolicies());
                for (Topic topic : applicationInstance.getLogicalTopics()) {
                    deployTopic(admin, (PulsarTopic) topic);
                }
            }
        }

        private static void applyRetentionPolicies(
                PulsarAdmin admin,
                String namespace,
                PulsarClusterRuntimeConfiguration.RetentionPolicies policies)
                throws PulsarAdminException {
            if (policies == null) {
                return;
            }
            if (policies.retentionSizeInMB() == null && policies.retentionTimeInMinutes() == null) {
                return;
            }
            admin.namespaces()
                    .setRetention(
                            namespace,
                            new RetentionPolicies(
                                    policies.retentionTimeInMinutes() == null
                                            ? -1
                                            : policies.retentionTimeInMinutes(),
                                    policies.retentionSizeInMB() == null
                                            ? -1
                                            : policies.retentionSizeInMB()));
        }

        private static void deployTopic(PulsarAdmin admin, PulsarTopic topic)
                throws PulsarAdminException {
            String createMode = topic.createMode();
            String namespace = topic.name().tenant() + "/" + topic.name().namespace();
            String topicName = topic.name().toPulsarName();
            log.info("Listing topics in namespace {}", namespace);
            List<String> existing;
            if (topic.partitions() <= 0) {
                existing = admin.topics().getList(namespace);
            } else {
                existing = admin.topics().getPartitionedTopicList(namespace);
            }
            log.info("Existing topics: {}", existing);
            String fullyQualifiedName = TopicName.get(topicName).toString();
            log.info("Looking for : {}", fullyQualifiedName);
            boolean exists = existing.contains(fullyQualifiedName);
            if (exists) {
                log.info("Topic {} already exists", topicName);
            } else {
                log.info("Topic {} does not exist", topicName);
            }
            switch (createMode) {
                case TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS -> {
                    if (!exists) {
                        log.info("Topic {} does not exist, creating", topicName);
                        if (topic.partitions() <= 0) {
                            admin.topics().createNonPartitionedTopic(topicName);
                        } else {
                            admin.topics().createPartitionedTopic(topicName, topic.partitions());
                        }
                    }
                }
                case TopicDefinition.CREATE_MODE_NONE -> {
                    // do nothing
                }
                default -> throw new IllegalArgumentException("Unknown create mode " + createMode);
            }

            // deploy schema
            if (topic.valueSchema() != null) {
                List<SchemaInfo> allSchemas = admin.schemas().getAllSchemas(topicName);
                if (allSchemas.isEmpty()) {
                    log.info("Deploying schema for topic {}: {}", topicName, topic.valueSchema());

                    SchemaInfo schemaInfo = getSchemaInfo(topic.valueSchema());
                    log.info("Value schema {}", schemaInfo);
                    if (topic.keySchema() != null) {
                        // KEY VALUE
                        log.info(
                                "Deploying key schema for topic {}: {}",
                                topicName,
                                topic.keySchema());
                        SchemaInfo keySchemaInfo = getSchemaInfo(topic.keySchema());
                        log.info("Key schema {}", keySchemaInfo);

                        schemaInfo =
                                KeyValueSchemaInfo.encodeKeyValueSchemaInfo(
                                        topic.valueSchema().name(),
                                        keySchemaInfo,
                                        schemaInfo,
                                        KeyValueEncodingType.SEPARATED);

                        log.info("KeyValue schema {}", schemaInfo);
                    }

                    admin.schemas().createSchema(topicName, schemaInfo);
                } else {
                    log.info(
                            "Topic {} already has some schemas, skipping. ({})",
                            topicName,
                            allSchemas);
                }
            }
        }

        private static SchemaInfo getSchemaInfo(SchemaDefinition logicalSchemaDefinition) {
            SchemaType pulsarSchemaType =
                    SchemaType.valueOf(logicalSchemaDefinition.type().toUpperCase());
            return SchemaInfo.builder()
                    .type(pulsarSchemaType)
                    .name(logicalSchemaDefinition.name())
                    .properties(Map.of())
                    .schema(
                            logicalSchemaDefinition.schema() != null
                                    ? logicalSchemaDefinition
                                            .schema()
                                            .getBytes(StandardCharsets.UTF_8)
                                    : new byte[0])
                    .build();
        }

        private static void deleteTopic(PulsarAdmin admin, PulsarTopic topic)
                throws PulsarAdminException {

            switch (topic.createMode()) {
                case TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS -> {}
                default -> {
                    log.info(
                            "Keeping Pulsar topic {} since creation-mode is {}",
                            topic.name(),
                            topic.createMode());
                    return;
                }
            }

            if (!topic.deleteMode().equals(TopicDefinition.DELETE_MODE_DELETE)) {
                log.info(
                        "Keeping Pulsar topic {} since deletion-mode is {}",
                        topic.name(),
                        topic.deleteMode());
                return;
            }

            String topicName =
                    topic.name().tenant()
                            + "/"
                            + topic.name().namespace()
                            + "/"
                            + topic.name().name();
            String fullyQualifiedName = TopicName.get(topicName).toString();
            log.info("Deleting topic {}", fullyQualifiedName);
            try {
                if (topic.partitions() <= 0) {
                    admin.topics().delete(fullyQualifiedName, true);
                } else {
                    admin.topics().deletePartitionedTopic(fullyQualifiedName, true);
                }
            } catch (PulsarAdminException.NotFoundException notFoundException) {
                log.info("Topic {} didn't exit. Not a problem", fullyQualifiedName);
            }
        }

        @Override
        @SneakyThrows
        public void delete(ExecutionPlan applicationInstance) {
            Application logicalInstance = applicationInstance.getApplication();
            try (PulsarAdmin admin =
                    buildPulsarAdmin(logicalInstance.getInstance().streamingCluster())) {
                for (Topic topic : applicationInstance.getLogicalTopics()) {
                    deleteTopic(admin, (PulsarTopic) topic);
                }
            }
        }

        @ToString
        private static class PulsarConsumerRecord implements Record {
            private final Object finalKey;
            private final Object finalValue;
            private final Message<GenericRecord> receive;

            public PulsarConsumerRecord(
                    Object finalKey, Object finalValue, Message<GenericRecord> receive) {
                this.finalKey = finalKey;
                this.finalValue = finalValue;
                this.receive = receive;
            }

            @Override
            public Object key() {
                return finalKey;
            }

            @Override
            public Object value() {
                return finalValue;
            }

            @Override
            public String origin() {
                return receive.getTopicName();
            }

            @Override
            public Long timestamp() {
                return receive.getPublishTime();
            }

            @Override
            public Collection<Header> headers() {
                return receive.getProperties().entrySet().stream()
                        .map(
                                e ->
                                        new Header() {
                                            @Override
                                            public String key() {
                                                return e.getKey();
                                            }

                                            @Override
                                            public String value() {
                                                return e.getValue();
                                            }

                                            @Override
                                            public String valueAsString() {
                                                return e.getValue();
                                            }
                                        })
                        .collect(Collectors.toList());
            }
        }

        private class PulsarTopicReader implements TopicReader {
            private final Map<String, Object> configuration;
            private final MessageId startMessageId;

            private Map<String, byte[]> topicMessageIds = new HashMap<>();

            private Reader<GenericRecord> reader;

            private PulsarTopicReader(
                    Map<String, Object> configuration, TopicOffsetPosition initialPosition) {
                this.configuration = configuration;
                this.startMessageId =
                        switch (initialPosition.position()) {
                            case Earliest -> MessageId.earliest;
                            case Latest, Absolute -> MessageId.latest;
                        };
                if (initialPosition.position() == TopicOffsetPosition.Position.Absolute) {
                    try {
                        this.topicMessageIds =
                                ObjectMapperFactory.getDefaultMapper()
                                        .readerForMapOf(byte[].class)
                                        .readValue(initialPosition.offset());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            @Override
            public void start() throws Exception {
                String topic = (String) configuration.remove("topic");
                reader =
                        client.newReader(Schema.AUTO_CONSUME())
                                .topic(topic)
                                .startMessageId(this.startMessageId)
                                .loadConf(configuration)
                                .receiverQueueSize(5) // To limit the number of messages in flight
                                .create();

                reader.seek(
                        topicPartition -> {
                            try {
                                String topicName = TopicName.get(topicPartition).toString();
                                return MessageId.fromByteArray(
                                        topicMessageIds.getOrDefault(
                                                topicName, startMessageId.toByteArray()));
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }

            @Override
            public void close() throws Exception {
                if (reader != null) {
                    reader.close();
                }
            }

            @Override
            public TopicReadResult read() throws Exception {
                Message<GenericRecord> receive = reader.readNext(1, TimeUnit.SECONDS);
                List<Record> records;
                byte[] offset;
                if (receive != null) {
                    Object key = receive.getKey();
                    Object value = receive.getValue().getNativeObject();
                    if (value instanceof KeyValue<?, ?> kv) {
                        key = kv.getKey();
                        value = kv.getValue();
                    }

                    final Object finalKey = key;
                    final Object finalValue = value;
                    log.info("Received message: {}", receive);
                    records = List.of(new PulsarConsumerRecord(finalKey, finalValue, receive));
                    topicMessageIds.put(
                            receive.getTopicName(), receive.getMessageId().toByteArray());
                    offset =
                            ObjectMapperFactory.getDefaultMapper()
                                    .writeValueAsBytes(topicMessageIds);
                } else {
                    records = List.of();
                    offset = null;
                }
                return new TopicReadResult() {
                    @Override
                    public List<Record> records() {
                        return records;
                    }

                    @Override
                    public byte[] offset() {
                        return offset;
                    }
                };
            }
        }

        private class PulsarTopicConsumer implements TopicConsumer {

            private final Map<String, Object> configuration;
            Consumer<GenericRecord> consumer;

            private final AtomicLong totalOut = new AtomicLong();

            public PulsarTopicConsumer(Map<String, Object> configuration) {
                this.configuration = configuration;
            }

            @Override
            public Object getNativeConsumer() {
                return consumer;
            }

            @Override
            public void start() throws Exception {
                String topic = (String) configuration.remove("topic");
                Integer receiverQueueSize = (Integer) configuration.remove("receiverQueueSize");
                consumer =
                        client.newConsumer(Schema.AUTO_CONSUME())
                                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                                .subscriptionType(SubscriptionType.Failover)
                                .loadConf(configuration)
                                .topic(topic)
                                .receiverQueueSize(
                                        receiverQueueSize == null
                                                ? 5
                                                : receiverQueueSize) // To limit the number of
                                // messages in flight
                                .subscribe();
            }

            @Override
            public void close() throws Exception {
                if (consumer != null) {
                    consumer.close();
                }
            }

            @Override
            public long getTotalOut() {
                return totalOut.get();
            }

            @Override
            public List<Record> read() throws Exception {
                Message<GenericRecord> receive = consumer.receive(1, TimeUnit.SECONDS);
                if (receive == null) {
                    return List.of();
                }
                Object key = receive.getKey();
                Object value = receive.getValue().getNativeObject();
                if (value instanceof KeyValue<?, ?> kv) {
                    key = kv.getKey();
                    value = kv.getValue();
                }

                final Object finalKey = key;
                final Object finalValue = value;
                if (log.isDebugEnabled()) {
                    log.debug("Received message, key: {}, value: {}", finalKey, finalValue);
                }
                totalOut.incrementAndGet();
                return List.of(new PulsarConsumerRecord(finalKey, finalValue, receive));
            }

            @Override
            public void commit(List<Record> records) throws Exception {
                for (Record record : records) {
                    if (record instanceof PulsarConsumerRecord pulsarConsumerRecord) {
                        consumer.acknowledge(pulsarConsumerRecord.receive.getMessageId());
                    } else {
                        log.error("Cannot commit record of type {}", record.getClass());
                    }
                }
            }
        }

        private class PulsarTopicProducer<K> implements TopicProducer {

            private final Map<String, Object> configuration;
            private final AtomicLong totalIn = new AtomicLong();
            volatile String topic;
            volatile Producer<K> producer;
            volatile Schema<K> schema;

            static final Map<Class<?>, Schema<?>> BASE_SCHEMAS =
                    Map.ofEntries(
                            entry(String.class, Schema.STRING),
                            entry(Map.class, Schema.STRING),
                            entry(Collection.class, Schema.STRING),
                            entry(Boolean.class, Schema.BOOL),
                            entry(Byte.class, Schema.INT8),
                            entry(Short.class, Schema.INT16),
                            entry(Integer.class, Schema.INT32),
                            entry(Long.class, Schema.INT64),
                            entry(Float.class, Schema.FLOAT),
                            entry(Double.class, Schema.DOUBLE),
                            entry(byte[].class, Schema.BYTES),
                            entry(Date.class, Schema.DATE),
                            entry(Timestamp.class, Schema.TIMESTAMP),
                            entry(Time.class, Schema.TIME),
                            entry(LocalDate.class, Schema.LOCAL_DATE),
                            entry(LocalTime.class, Schema.LOCAL_TIME),
                            entry(LocalDateTime.class, Schema.LOCAL_DATE_TIME),
                            entry(Instant.class, Schema.INSTANT),
                            entry(ByteBuffer.class, Schema.BYTEBUFFER));

            public PulsarTopicProducer(Map<String, Object> configuration) {
                this.configuration = configuration;
            }

            @Override
            @SneakyThrows
            public void start() {
                synchronized (this) {
                    if (topic != null) {
                        return;
                    }
                    String localTopic = (String) configuration.remove("topic");
                    Objects.requireNonNull(localTopic, "topic is required");
                    Optional<SchemaInfo> existingSchema =
                            ((PulsarClientImpl) client)
                                    .getSchema(localTopic)
                                    .get(30, TimeUnit.SECONDS);
                    if (existingSchema.isPresent()) {
                        log.info("Found existing schema from topic {}", localTopic);
                        schema = (Schema<K>) Schema.getSchema(existingSchema.get());
                        configuration.remove("valueSchema");
                        configuration.remove("keySchema");
                    } else {
                        if (configuration.containsKey("valueSchema")) {
                            log.info("Using schema from topic definition {}", localTopic);
                            SchemaDefinition valueSchemaDefinition =
                                    ObjectMapperFactory.getDefaultMapper()
                                            .convertValue(
                                                    configuration.remove("valueSchema"),
                                                    SchemaDefinition.class);
                            Schema<?> valueSchema =
                                    Schema.getSchema(getSchemaInfo(valueSchemaDefinition));
                            if (configuration.containsKey("keySchema")) {
                                SchemaDefinition keySchemaDefinition =
                                        ObjectMapperFactory.getDefaultMapper()
                                                .convertValue(
                                                        configuration.remove("keySchema"),
                                                        SchemaDefinition.class);
                                Schema<?> keySchema =
                                        Schema.getSchema(getSchemaInfo(keySchemaDefinition));
                                schema =
                                        (Schema<K>)
                                                Schema.KeyValue(
                                                        keySchema,
                                                        valueSchema,
                                                        KeyValueEncodingType.SEPARATED);
                            } else {
                                schema = (Schema<K>) valueSchema;
                            }
                        }
                    }
                    log.info("Schema is {}", schema);
                    topic = localTopic;
                    if (schema != null) {
                        initializeProducer();
                    }
                }
            }

            @Override
            public Object getNativeProducer() {
                return producer;
            }

            @Override
            @SneakyThrows
            public void close() {
                if (producer != null) {
                    producer.close();
                }
            }

            private static Schema<?> getSchema(Class<?> klass) {
                Schema<?> schema = BASE_SCHEMAS.get(klass);
                if (schema == null) {
                    for (Map.Entry<Class<?>, Schema<?>> classSchemaEntry :
                            BASE_SCHEMAS.entrySet()) {
                        if (classSchemaEntry.getKey().isAssignableFrom(klass)) {
                            return classSchemaEntry.getValue();
                        }
                    }
                    throw new IllegalArgumentException("Cannot infer schema for " + klass);
                }
                return schema;
            }

            @Override
            public CompletableFuture<?> write(Record r) {
                if (topic == null) {
                    throw new RuntimeException("PulsarTopicProducer not started");
                }
                totalIn.addAndGet(1);
                // On the first write with no schema, infer the schema from the first message
                // and initialize the producer. For subsequent writes, the producer the schema
                // is set so a new producer is not started
                // Synchronize the initialization of the schema and producer
                if (producer == null) {
                    synchronized (this) {
                        // Double-check idiom to avoid race conditions
                        if (producer == null) {
                            try {
                                inferSchemaFromRecord(r);
                                initializeProducer();
                            } catch (Throwable e) {
                                return CompletableFuture.failedFuture(e);
                            }
                        }
                    }
                }
                log.info("Writing message {} to topic {} with schema {}", r, topic, schema);

                Map<String, String> properties = new HashMap<>();
                for (Header header : r.headers()) {
                    properties.put(header.key(), header.valueAsString());
                }
                TypedMessageBuilder<K> message = producer.newMessage().properties(properties);

                if (schema instanceof KeyValueSchema<?, ?> keyValueSchema) {
                    KeyValue<?, ?> keyValue =
                            new KeyValue<>(
                                    convertValue(r.key(), keyValueSchema.getKeySchema()),
                                    convertValue(r.value(), keyValueSchema.getValueSchema()));
                    message.value((K) keyValue);
                } else {
                    if (r.key() != null) {
                        message.key(r.key().toString());
                    }
                    message.value((K) convertValue(r.value(), schema));
                }

                return message.sendAsync();
            }

            private void inferSchemaFromRecord(Record r) {
                final Schema<?> valueSchema;
                if (r.value() != null) {
                    valueSchema = getSchema(r.value().getClass());
                } else {
                    valueSchema = Schema.BYTES;
                }
                if (r.key() != null) {
                    Schema<?> keySchema = getSchema(r.key().getClass());
                    schema =
                            (Schema<K>)
                                    Schema.KeyValue(
                                            keySchema, valueSchema, KeyValueEncodingType.SEPARATED);
                } else {
                    schema = (Schema<K>) valueSchema;
                }
                log.info("Inferred schema from record {} -> {}", r, schema);
            }

            private static Object convertValue(Object value, Schema<?> schema) {
                if (value == null) {
                    return null;
                }
                switch (schema.getSchemaInfo().getType()) {
                    case BYTES:
                        if (value instanceof byte[]) {
                            return value;
                        }
                        return value.toString().getBytes(StandardCharsets.UTF_8);
                    case STRING:
                        if (value instanceof String) {
                            return value;
                        }
                        if (Map.class.isAssignableFrom(value.getClass())
                                || Collection.class.isAssignableFrom(value.getClass())) {
                            try {
                                return ObjectMapperFactory.getDefaultMapper()
                                        .writeValueAsString(value);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        return value.toString();
                    default:
                        throw new IllegalArgumentException(
                                "Unsupported output schema type " + schema);
                }
            }

            private void initializeProducer() throws Exception {
                final int maxAttempts = 6;
                for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                    try {
                        log.info("Starting initialization of new producer for topic {}", topic);
                        producer =
                                client.newProducer(schema)
                                        .topic(topic)
                                        .loadConf(configuration)
                                        .create();
                        log.info("Producer successfully initialized for topic {}", topic);
                        return;
                    } catch (Exception e) {
                        log.error("Failed to initialize producer on attempt " + attempt, e);
                        if (attempt < maxAttempts) {
                            log.info(
                                    "Retrying to initialize producer... Attempt "
                                            + (attempt + 1)
                                            + " of "
                                            + maxAttempts);
                            Thread.sleep(500);
                        } else {
                            throw e;
                        }
                    }
                }
            }

            @Override
            public long getTotalIn() {
                return totalIn.get();
            }
        }
    }
}
