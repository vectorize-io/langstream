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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.langstream.api.runner.code.*;
import ai.langstream.api.runner.code.Record;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class AgentRunnerTest {

    @Test
    void skip() throws Exception {
        SimpleSource source = new SimpleSource(List.of(SimpleRecord.of("key", "fail-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 0, "onFailure", "skip"));
        AgentContext context = createMockAgentContext();

        runMainLoopSync(source, processor, sink, context, errorHandler, source::hasMoreRecords);
        processor.expectExecutions(1);
        source.expectUncommitted(0);
    }

    private static AgentContext createMockAgentContext() {
        AgentContext context = mock(AgentContext.class);
        when(context.getMetricsReporter()).thenReturn(MetricsReporter.DISABLED);
        return context;
    }

    @Test
    void failWithRetries() {
        SimpleSource source = new SimpleSource(List.of(SimpleRecord.of("key", "fail-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 3, "onFailure", "fail"));
        AgentContext context = createMockAgentContext();
        when(context.getMetricsReporter()).thenReturn(MetricsReporter.DISABLED);
        assertThrows(
                AgentRunner.PermanentFailureException.class,
                () ->
                        runMainLoopSync(
                                source,
                                processor,
                                sink,
                                context,
                                errorHandler,
                                source::hasMoreRecords));
        processor.expectExecutions(3);
        source.expectUncommitted(1);
    }

    @Test
    void failNoRetries() {
        SimpleSource source = new SimpleSource(List.of(SimpleRecord.of("key", "fail-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 0, "onFailure", "fail"));
        AgentContext context = createMockAgentContext();
        assertThrows(
                AgentRunner.PermanentFailureException.class,
                () ->
                        runMainLoopSync(
                                source,
                                processor,
                                sink,
                                context,
                                errorHandler,
                                source::hasMoreRecords));
        processor.expectExecutions(1);
        source.expectUncommitted(1);
    }

    @Test
    void someFailedSomeGoodWithSkip() throws Exception {
        SimpleSource source =
                new SimpleSource(
                        List.of(
                                SimpleRecord.of("key", "fail-me"),
                                SimpleRecord.of("key", "process-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 0, "onFailure", "skip"));
        AgentContext context = createMockAgentContext();
        runMainLoopSync(source, processor, sink, context, errorHandler, source::hasMoreRecords);
        processor.expectExecutions(2);
        source.expectUncommitted(0);
    }

    @Test
    void someGoodSomeFailedWithSkip() throws Exception {
        SimpleSource source =
                new SimpleSource(
                        List.of(
                                SimpleRecord.of("key", "process-me"),
                                SimpleRecord.of("key", "fail-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 0, "onFailure", "skip"));
        AgentContext context = createMockAgentContext();
        runMainLoopSync(source, processor, sink, context, errorHandler, source::hasMoreRecords);
        processor.expectExecutions(2);
        source.expectUncommitted(0);
    }

    @Test
    void someGoodSomeFailedWithRetry() throws Exception {
        SimpleSource source =
                new SimpleSource(
                        List.of(
                                SimpleRecord.of("key", "process-me"),
                                SimpleRecord.of("key", "fail-me")));
        AgentSink sink = new SimpleSink();
        FailingAgentProcessor processor = new FailingAgentProcessor(Set.of("fail-me"), 3);
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 5, "onFailure", "fail"));
        AgentContext context = createMockAgentContext();
        runMainLoopSync(source, processor, sink, context, errorHandler, source::hasMoreRecords);
        processor.expectExecutions(5);
        source.expectUncommitted(0);
    }

    @Test
    void someGoodSomeFailedWithSkipAndBatching() throws Exception {
        SimpleSource source =
                new SimpleSource(
                        2,
                        List.of(
                                SimpleRecord.of("key", "process-me"),
                                SimpleRecord.of("key", "fail-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 0, "onFailure", "skip"));
        AgentContext context = createMockAgentContext();
        runMainLoopSync(source, processor, sink, context, errorHandler, source::hasMoreRecords);
        processor.expectExecutions(2);
        source.expectUncommitted(0);
    }

    @Test
    void someFailedSomeGoodWithSkipAndBatching() throws Exception {
        SimpleSource source =
                new SimpleSource(
                        2,
                        List.of(
                                SimpleRecord.of("key", "fail-me"),
                                SimpleRecord.of("key", "process-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 0, "onFailure", "skip"));
        AgentContext context = createMockAgentContext();
        runMainLoopSync(source, processor, sink, context, errorHandler, source::hasMoreRecords);
        // all the records are processed in one batch
        processor.expectExecutions(2);
        source.expectUncommitted(0);
    }

    @Test
    void someFailedSomeGoodWithRetryAndBatching() throws Exception {
        SimpleSource source =
                new SimpleSource(
                        2,
                        List.of(
                                SimpleRecord.of("key", "fail-me"),
                                SimpleRecord.of("key", "process-me")));
        AgentSink sink = new SimpleSink();
        FailingAgentProcessor processor = new FailingAgentProcessor(Set.of("fail-me"), 2);
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 3, "onFailure", "fail"));
        AgentContext context = createMockAgentContext();
        runMainLoopSync(source, processor, sink, context, errorHandler, source::hasMoreRecords);
        // all the records are processed in one batch
        processor.expectExecutions(4);
        source.expectUncommitted(0);
    }

    private static class SimpleSink extends AbstractAgentCode implements AgentSink {
        @Override
        public CompletableFuture<?> write(Record record) {
            return CompletableFuture.completedFuture(null);
        }
    }

    private static class SimpleSource extends AbstractAgentCode implements AgentSource {

        final List<Record> records;
        final List<Record> uncommitted = new ArrayList<>();

        final int batchSize;

        public SimpleSource(int batchSize, List<Record> records) {
            this.batchSize = batchSize;
            this.records = new ArrayList<>(records);
        }

        public SimpleSource(List<Record> records) {
            this(1, records);
        }

        synchronized boolean hasMoreRecords() {
            return !records.isEmpty();
        }

        @Override
        public synchronized List<Record> read() {
            if (records.isEmpty()) {
                return List.of();
            }
            List<Record> result = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                Record remove = records.remove(0);
                result.add(remove);
                uncommitted.add(remove);
                if (records.isEmpty()) {
                    break;
                }
            }
            return result;
        }

        @Override
        public synchronized void commit(List<Record> records) {
            System.out.println("COMMIT " + records + " UNCOMMITTED " + uncommitted);
            uncommitted.removeAll(records);
            System.out.println("AFTER COMMIT UNCOMMITTED " + uncommitted);
        }

        synchronized void expectUncommitted(int count) {
            assertEquals(count, uncommitted.size());
        }
    }

    public class FailingAgentProcessor extends SingleRecordAgentProcessor {
        private final Set<String> failOnContent;
        private final int maxFailures; // Maximum number of times to fail before succeeding
        private final Map<String, Integer> failureCounts = new HashMap<>();
        private int executionCount;

        public FailingAgentProcessor(Set<String> failOnContent, int maxFailures) {
            this.failOnContent = failOnContent;
            this.maxFailures = maxFailures;
        }

        @Override
        public List<Record> processRecord(Record record) {
            log.info("Processing {}", record.value());
            executionCount++;
            String recordValue = (String) record.value();

            // Update failure count for this record
            int currentFailures = failureCounts.getOrDefault(recordValue, 0);
            failureCounts.put(recordValue, currentFailures + 1);

            // Check if the record should fail this time
            if (failOnContent.contains(recordValue) && currentFailures < maxFailures) {
                log.info(
                        "Record {} failed intentionally, attempt {}", recordValue, currentFailures);
                throw new RuntimeException("Failed on " + recordValue);
            }

            // If it has failed the maximum times, or it's not set to fail, it processes
            // successfully
            return List.of(record);
        }

        void expectExecutions(int count) {
            assertEquals(count, executionCount);
        }
    }

    private static class SimpleAgentProcessor extends SingleRecordAgentProcessor {

        private final Set<String> failOnContent;
        private int executionCount;

        public SimpleAgentProcessor(Set<String> failOnContent) {
            this.failOnContent = failOnContent;
        }

        @Override
        public List<Record> processRecord(Record record) {
            log.info("Processing {}", record.value());
            executionCount++;
            if (failOnContent.contains((String) record.value())) {
                throw new RuntimeException("Failed on " + record.value());
            }
            return List.of(record);
        }

        void expectExecutions(int count) {
            assertEquals(count, executionCount);
        }
    }

    private static void runMainLoopSync(
            AgentSource source,
            AgentProcessor processor,
            AgentSink sink,
            AgentContext agentContext,
            ErrorsHandler errorsHandler,
            Supplier<Boolean> continueLoop)
            throws Exception {
        final ExecutorService executorService = Executors.newCachedThreadPool();
        AgentRunner.runMainLoop(
                source,
                processor,
                sink,
                agentContext,
                errorsHandler,
                continueLoop,
                executorService);

        executorService.shutdown();
        executorService.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS);
    }
}
