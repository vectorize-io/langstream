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
package ai.langstream.runtime.agent;

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.api.runner.code.*;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TopicConsumerSource extends AbstractAgentCode implements AgentSource {

    private final TopicConsumer consumer;
    private final TopicProducer deadLetterQueueProducer;

    public TopicConsumerSource(TopicConsumer consumer, TopicProducer deadLetterQueueProducer) {
        this.consumer = consumer;
        this.deadLetterQueueProducer = deadLetterQueueProducer;
    }

    @Override
    public List<Record> read() throws Exception {
        List<Record> result = consumer.read();
        processed(0, result.size());
        return result;
    }

    @Override
    public void commit(List<Record> records) throws Exception {
        consumer.commit(records);
    }

    @Override
    public void permanentFailure(Record record, Exception error, ErrorTypes errorType) {
        errorType = errorType == null ? ErrorTypes.INTERNAL_ERROR : errorType;
        log.error("Permanent {} failure on record {}", errorType, record, error);
        MutableRecord recordWithError = MutableRecord.recordToMutableRecord(record, false);
        String sourceTopic = record.origin();
        recordWithError.setProperty(
                SystemHeaders.ERROR_HANDLING_ERROR_TYPE.getKey(), errorType.toString());

        recordWithError.setProperty(
                SystemHeaders.ERROR_HANDLING_SOURCE_TOPIC.getKey(), sourceTopic);
        recordWithError.setProperty(
                SystemHeaders.ERROR_HANDLING_SOURCE_TOPIC_LEGACY.getKey(), sourceTopic);

        recordWithError.setProperty(
                SystemHeaders.ERROR_HANDLING_ERROR_MESSAGE.getKey(), error.getMessage());
        recordWithError.setProperty(
                SystemHeaders.ERROR_HANDLING_ERROR_MESSAGE_LEGACY.getKey(), error.getMessage());
        recordWithError.setProperty(
                SystemHeaders.ERROR_HANDLING_ERROR_CLASS.getKey(), error.getClass().getName());
        recordWithError.setProperty(
                SystemHeaders.ERROR_HANDLING_ERROR_CLASS_LEGACY.getKey(),
                error.getClass().getName());

        Throwable cause = error.getCause();
        if (cause != null) {
            recordWithError.setProperty(
                    SystemHeaders.ERROR_HANDLING_CAUSE_ERROR_MESSAGE.getKey(), cause.getMessage());
            recordWithError.setProperty(
                    SystemHeaders.ERROR_HANDLING_CAUSE_ERROR_MESSAGE_LEGACY.getKey(),
                    cause.getMessage());
            recordWithError.setProperty(
                    SystemHeaders.ERROR_HANDLING_CAUSE_ERROR_CLASS.getKey(),
                    cause.getClass().getName());
            recordWithError.setProperty(
                    SystemHeaders.ERROR_HANDLING_CAUSE_ERROR_CLASS_LEGACY.getKey(),
                    cause.getClass().getName());
            Throwable rootCause = cause;
            while (rootCause.getCause() != null) {
                rootCause = rootCause.getCause();
            }
            recordWithError.setProperty(
                    SystemHeaders.ERROR_HANDLING_ROOT_CAUSE_ERROR_MESSAGE.getKey(),
                    rootCause.getMessage());
            recordWithError.setProperty(
                    SystemHeaders.ERROR_HANDLING_ROOT_CAUSE_ERROR_MESSAGE_LEGACY.getKey(),
                    rootCause.getMessage());
            recordWithError.setProperty(
                    SystemHeaders.ERROR_HANDLING_ROOT_CAUSE_ERROR_CLASS.getKey(),
                    rootCause.getClass().getName());
            recordWithError.setProperty(
                    SystemHeaders.ERROR_HANDLING_ROOT_CAUSE_ERROR_CLASS_LEGACY.getKey(),
                    rootCause.getClass().getName());
        }
        Record finalRecord = MutableRecord.mutableRecordToRecord(recordWithError).get();
        log.info("Writing to DLQ: {}", finalRecord);
        deadLetterQueueProducer.write(finalRecord).join();
    }

    @Override
    public void start() throws Exception {
        consumer.start();
        log.info("Starting consumer {}", consumer);
        deadLetterQueueProducer.start();
    }

    @Override
    public void close() throws Exception {
        super.close();
        log.info("Closing consumer {}", consumer);
        consumer.close();
        deadLetterQueueProducer.close();
    }

    @Override
    public String toString() {
        return "TopicConsumerSource{" + "consumer=" + consumer + '}';
    }

    @Override
    protected Map<String, Object> buildAdditionalInfo() {
        return Map.of("consumer", consumer.getInfo());
    }
}
