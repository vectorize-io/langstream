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
package ai.langstream.kafka;

import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.kafka.extensions.KafkaContainerExtension;
import ai.langstream.kafka.extensions.KafkaRegistryContainerExtension;
import ai.langstream.testrunners.AbstractGenericStreamingApplicationRunner;
import ai.langstream.testrunners.kafka.KafkaApplicationRunner;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import junit.framework.AssertionFailedError;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.KafkaContainer;

@Slf4j
class KafkaSchemaTest extends AbstractGenericStreamingApplicationRunner {

    public KafkaSchemaTest() {
        super("kafka");
    }

    @RegisterExtension
    static KafkaRegistryContainerExtension registry =
            new KafkaRegistryContainerExtension(new KafkaContainerExtension());

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static final class Pojo {

        private String name;
    }

    @Test
    public void testUseSchemaWithKafka() throws Exception {
        String schemaDefinition =
                """
                        {
                          "type" : "record",
                          "name" : "Pojo",
                          "namespace" : "mynamespace",
                          "fields" : [ {
                            "name" : "name",
                            "type" : "string"
                          } ]
                        }
                """;
        Schema schema = new Schema.Parser().parse(schemaDefinition);
        GenericRecord record = new GenericData.Record(schema);
        record.put("name", "foo");
        log.info("Schema: {}", schemaDefinition);
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                    schema:
                                      type: avro
                                      schema: '%s'
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                    schema:
                                      type: avro
                                      schema: |
                                           {
                                              "type" : "record",
                                              "name" : "Pojo",
                                              "namespace" : "mynamespace",
                                              "fields" : [ {
                                                "name" : "name",
                                                "type" : "string"
                                              } ]
                                            }
                                pipeline:
                                  - name: "identity"
                                    id: "step1"
                                    type: "identity"
                                    input: "input-topic"
                                    output: "output-topic"
                                """
                                .formatted(schemaDefinition));
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, GenericRecord> producer = createAvroProducer();
                    KafkaConsumer<String, String> consumer = createAvroConsumer("output-topic")) {
                producer.send(
                                new ProducerRecord<>(
                                        "input-topic",
                                        null,
                                        System.currentTimeMillis(),
                                        "key",
                                        record,
                                        List.of()))
                        .get();
                producer.flush();

                executeAgentRunners(applicationRuntime);

                waitForMessages(consumer, List.of(record));
            }
        }
    }

    @Override
    protected String buildInstanceYaml() {

        KafkaContainer kafkaContainer =
                ((KafkaApplicationRunner) streamingClusterRunner).getKafkaContainer();
        return """
                instance:
                  streamingCluster:
                    type: "kafka"
                    configuration:
                      admin:
                        bootstrap.servers: "%s"
                        schema.registry.url: "%s"
                  computeCluster:
                     type: "kubernetes"
                """
                .formatted(kafkaContainer.getBootstrapServers(), registry.getSchemaRegistryUrl());
    }

    protected KafkaProducer<String, GenericRecord> createAvroProducer() {
        KafkaContainer kafkaContainer =
                ((KafkaApplicationRunner) streamingClusterRunner).getKafkaContainer();
        return new KafkaProducer<>(
                Map.of(
                        "bootstrap.servers",
                        kafkaContainer.getBootstrapServers(),
                        "key.serializer",
                        "org.apache.kafka.common.serialization.StringSerializer",
                        "value.serializer",
                        KafkaAvroSerializer.class.getName(),
                        "schema.registry.url",
                        registry.getSchemaRegistryUrl()));
    }

    protected KafkaConsumer<String, String> createAvroConsumer(String topic) {
        KafkaContainer kafkaContainer =
                ((KafkaApplicationRunner) streamingClusterRunner).getKafkaContainer();
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(
                        Map.of(
                                "bootstrap.servers",
                                kafkaContainer.getBootstrapServers(),
                                "key.deserializer",
                                "org.apache.kafka.common.serialization.StringDeserializer",
                                "value.deserializer",
                                KafkaAvroDeserializer.class.getName(),
                                "group.id",
                                "testgroup",
                                "auto.offset.reset",
                                "earliest",
                                "schema.registry.url",
                                registry.getSchemaRegistryUrl()));
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    protected List<ConsumerRecord> waitForMessages(
            KafkaConsumer consumer,
            BiConsumer<List<ConsumerRecord>, List<Object>> assertionOnReceivedMessages) {
        List<ConsumerRecord> result = new ArrayList<>();
        List<Object> received = new ArrayList<>();

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            ConsumerRecords<String, String> poll =
                                    consumer.poll(Duration.ofSeconds(2));
                            for (ConsumerRecord record : poll) {
                                log.info("Received message {}", record);
                                received.add(record.value());
                                result.add(record);
                            }
                            log.info("Result:  {}", received);
                            received.forEach(r -> log.info("Received |{}|", r));

                            try {
                                assertionOnReceivedMessages.accept(result, received);
                            } catch (AssertionFailedError assertionFailedError) {
                                log.info("Assertion failed", assertionFailedError);
                                throw assertionFailedError;
                            }
                        });

        return result;
    }

    protected List<ConsumerRecord> waitForMessages(KafkaConsumer consumer, List<?> expected) {
        return waitForMessages(
                consumer,
                (result, received) -> {
                    assertEquals(expected.size(), received.size());
                    for (int i = 0; i < expected.size(); i++) {
                        Object expectedValue = expected.get(i);
                        Object actualValue = received.get(i);
                        if (expectedValue instanceof Consumer fn) {
                            fn.accept(actualValue);
                        } else if (expectedValue instanceof byte[]) {
                            assertArrayEquals((byte[]) expectedValue, (byte[]) actualValue);
                        } else {
                            log.info("expected: {}", expectedValue);
                            log.info("got: {}", actualValue);
                            assertEquals(expectedValue, actualValue);
                        }
                    }
                });
    }
}
