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
package ai.langstream.agents.pulsardlq;

import ai.langstream.api.runner.code.*;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.util.ConfigurationUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;

@Slf4j
public class PulsarDLQSource extends AbstractAgentCode implements AgentSource {
    private String pulsarUrl;
    private String namespace;
    private String subscription;
    private String dlqSuffix;
    private PulsarClient pulsarClient;
    private Consumer<byte[]> dlqTopicsConsumer;
    private boolean includePartitioned;
    private int timeoutMs;
    private int autoDiscoveryPeriodSeconds;

    private static class PulsarRecord implements Record {
        private final Message<byte[]> message;
        private final Collection<Header> headers = new ArrayList<>();

        public PulsarRecord(Message<byte[]> message) {
            this.message = message;
            Map<String, String> properties = message.getProperties();
            if (properties != null) {
                for (Map.Entry<String, String> entry : properties.entrySet()) {
                    headers.add(new SimpleRecord.SimpleHeader(entry.getKey(), entry.getValue()));
                }
            }
        }

        @Override
        public Object key() {
            return message.getKey();
        }

        @Override
        public Object value() {
            return message.getData();
        }

        public MessageId messageId() {
            return message.getMessageId();
        }

        @Override
        public String origin() {
            return message.getTopicName(); // Static or derived from message properties
        }

        @Override
        public Long timestamp() {
            return message.getPublishTime(); // Using publish time as timestamp
        }

        @Override
        public Collection<Header> headers() {
            return headers;
        }

        @Override
        public String toString() {
            return "PulsarRecord{"
                    + "key="
                    + key()
                    + ", value="
                    + value()
                    + ", headers="
                    + headers()
                    + '}';
        }
    }

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        pulsarUrl = ConfigurationUtils.getString("pulsar-url", "", configuration);
        namespace = ConfigurationUtils.getString("namespace", "", configuration);
        subscription = ConfigurationUtils.getString("subscription", "", configuration);
        dlqSuffix = ConfigurationUtils.getString("dlq-suffix", "-DLQ", configuration);
        includePartitioned =
                ConfigurationUtils.getBoolean("include-partitioned", false, configuration);
        timeoutMs = ConfigurationUtils.getInt("timeout-ms", 0, configuration);
        autoDiscoveryPeriodSeconds =
                ConfigurationUtils.getInt(
                        "pattern-auto-discovery-period-seconds", 60, configuration);
    }

    @Override
    public void start() throws Exception {
        log.info("Starting pulsar client {}", pulsarUrl);
        pulsarClient = PulsarClient.builder().serviceUrl(pulsarUrl).build();
        // The maximum length of the regex is 50 characters
        // Using the persistent:// prefix generally works better, but
        // it can push the partitioned pattern over the 50 characters limit, so
        // we drop it for partitioned topics
        String patternString = "persistent://" + namespace + "/.*" + dlqSuffix;
        if (includePartitioned) {
            patternString = namespace + "/.*" + dlqSuffix + "(-p.+-\\d+)?";
        }

        Pattern dlqTopicsInNamespace = Pattern.compile(patternString);

        try {
            dlqTopicsConsumer =
                    pulsarClient
                            .newConsumer()
                            .consumerName("dlq-source")
                            .patternAutoDiscoveryPeriod(
                                    autoDiscoveryPeriodSeconds, TimeUnit.SECONDS)
                            .topicsPattern(dlqTopicsInNamespace)
                            .subscriptionName(subscription)
                            .subscribe();
        } catch (PulsarClientException pulsarClientException) {
            log.error("Error creating consumer", pulsarClientException);
            throw pulsarClientException;
        }
    }

    @Override
    public void close() {
        super.close();
        if (dlqTopicsConsumer != null) {
            try {
                dlqTopicsConsumer.close();
            } catch (PulsarClientException pulsarClientException) {
                log.error("Error closing consumer", pulsarClientException);
            }
        }
        if (pulsarClient != null) {
            try {
                pulsarClient.close();
            } catch (PulsarClientException pulsarClientException) {
                log.error("Error closing client", pulsarClientException);
            }
        }
    }

    @Override
    public List<Record> read() throws Exception {
        Message<byte[]> msg;
        if (timeoutMs > 0) {
            msg = dlqTopicsConsumer.receive(timeoutMs, TimeUnit.MILLISECONDS);
        } else {
            msg = dlqTopicsConsumer.receive();
        }
        if (msg == null) {
            log.debug("No message received");
            return List.of();
        }
        log.info("Received message: {}", new String(msg.getData()));
        Record record = new PulsarRecord(msg);
        return List.of(record);
    }

    @Override
    public void commit(List<Record> records) throws Exception {
        for (Record r : records) {
            PulsarRecord record = (PulsarRecord) r;
            dlqTopicsConsumer.acknowledge(record.messageId());
        }
    }

    @Override
    public void permanentFailure(Record record, Exception error, ErrorTypes errorType)
            throws Exception {
        PulsarRecord pulsarRecord = (PulsarRecord) record;
        log.error("Failure on record {}", pulsarRecord, error);
        dlqTopicsConsumer.negativeAcknowledge(pulsarRecord.messageId());
    }
}
