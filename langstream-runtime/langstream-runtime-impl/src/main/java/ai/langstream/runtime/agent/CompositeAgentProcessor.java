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

import ai.langstream.api.runner.code.*;
import ai.langstream.api.runner.code.Record;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;

/** This is a special processor that executes a pipeline of Agents in memory. */
@Slf4j
public class CompositeAgentProcessor extends AbstractAgentCode implements AgentProcessor {

    private AgentSource source;
    private final List<AgentProcessor> processors = new ArrayList<>();
    private AgentSink sink;

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        AgentCodeRegistry agentCodeRegistry =
                Objects.requireNonNull(getAgentCodeRegistry(), "agentCodeRegistry is required");
        List<Map<String, Object>> processorsDefinition =
                (List<Map<String, Object>>) configuration.getOrDefault("processors", List.of());
        Map<String, Object> sourceDefinition =
                (Map<String, Object>) configuration.getOrDefault("source", Map.of());
        Map<String, Object> sinkDefinition =
                (Map<String, Object>) configuration.getOrDefault("sink", Map.of());

        if (!sourceDefinition.isEmpty()) {
            String agentId1 = (String) sourceDefinition.get("agentId");
            String agentType1 = (String) sourceDefinition.get("agentType");
            Map<String, Object> agentConfiguration =
                    (Map<String, Object>) sourceDefinition.get("configuration");
            source =
                    AgentRunner.initAgent(
                                    agentId1,
                                    agentType1,
                                    startedAt(),
                                    agentConfiguration,
                                    agentCodeRegistry)
                            .asSource();
        }

        for (Map<String, Object> agentDefinition : processorsDefinition) {
            String agentId1 = (String) agentDefinition.get("agentId");
            String agentType1 = (String) agentDefinition.get("agentType");
            Map<String, Object> agentConfiguration =
                    (Map<String, Object>) agentDefinition.get("configuration");
            AgentProcessor agent =
                    AgentRunner.initAgent(
                                    agentId1,
                                    agentType1,
                                    startedAt(),
                                    agentConfiguration,
                                    agentCodeRegistry)
                            .asProcessor();
            processors.add(agent);
        }

        if (!sinkDefinition.isEmpty()) {
            String agentId1 = (String) sinkDefinition.get("agentId");
            String agentType1 = (String) sinkDefinition.get("agentType");
            Map<String, Object> agentConfiguration =
                    (Map<String, Object>) sinkDefinition.get("configuration");
            sink =
                    AgentRunner.initAgent(
                                    agentId1,
                                    agentType1,
                                    startedAt(),
                                    agentConfiguration,
                                    agentCodeRegistry)
                            .asSink();
        }
    }

    public AgentSource getSource() {
        return source;
    }

    public AgentSink getSink() {
        return sink;
    }

    @Override
    public void setContext(AgentContext context) throws Exception {
        super.setContext(context);
        for (AgentProcessor agent : processors) {
            agent.setContext(context);
        }
    }

    @Override
    public void start() throws Exception {
        for (AgentProcessor agent : processors) {
            agent.start();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        for (AgentProcessor agent : processors) {
            agent.close();
        }
    }

    @Override
    public void cleanup(Map<String, Object> configuration, AgentContext context) throws Exception {
        AgentCodeRegistry agentCodeRegistry =
                Objects.requireNonNull(getAgentCodeRegistry(), "agentCodeRegistry is required");
        List<Map<String, Object>> processorsDefinition =
                (List<Map<String, Object>>) configuration.getOrDefault("processors", List.of());
        Map<String, Object> sourceDefinition =
                (Map<String, Object>) configuration.getOrDefault("source", Map.of());
        Map<String, Object> sinkDefinition =
                (Map<String, Object>) configuration.getOrDefault("sink", Map.of());

        if (!sourceDefinition.isEmpty()) {
            String agentId1 = (String) sourceDefinition.get("agentId");
            String agentType1 = (String) sourceDefinition.get("agentType");
            Map<String, Object> agentConfiguration =
                    (Map<String, Object>) sourceDefinition.get("configuration");
            cleanup(context, agentCodeRegistry, agentType1, agentId1, agentConfiguration);
        }

        for (Map<String, Object> agentDefinition : processorsDefinition) {
            String agentId1 = (String) agentDefinition.get("agentId");
            String agentType1 = (String) agentDefinition.get("agentType");
            Map<String, Object> agentConfiguration =
                    (Map<String, Object>) agentDefinition.get("configuration");
            cleanup(context, agentCodeRegistry, agentType1, agentId1, agentConfiguration);
        }

        if (!sinkDefinition.isEmpty()) {
            String agentId1 = (String) sinkDefinition.get("agentId");
            String agentType1 = (String) sinkDefinition.get("agentType");
            Map<String, Object> agentConfiguration =
                    (Map<String, Object>) sinkDefinition.get("configuration");
            cleanup(context, agentCodeRegistry, agentType1, agentId1, agentConfiguration);
        }
    }

    private void cleanup(
            AgentContext context,
            AgentCodeRegistry agentCodeRegistry,
            String agentType1,
            String agentId1,
            Map<String, Object> agentConfiguration)
            throws Exception {
        AgentCodeAndLoader agentCodeAndLoader = agentCodeRegistry.getAgentCode(agentType1);
        agentCodeAndLoader.executeWithContextClassloader(
                (AgentCode agentCode) -> {
                    agentCode.setMetadata(agentId1, agentType1, startedAt());
                    agentCode.setAgentCodeRegistry(agentCodeRegistry);
                    agentCode.cleanup(agentConfiguration, context);
                });
    }

    /**
     * This method executes the pipeline, starting from a single SourceRecord. It is possible that
     * each step of the pipeline generates multiple records.
     *
     * @param index
     * @param currentRecords
     * @param initialSourceRecord
     * @param finalStep
     */
    private void invokeProcessor(
            int index,
            List<Record> currentRecords,
            Record initialSourceRecord,
            RecordSink finalStep) {
        AgentProcessor processor = processors.get(index);
        try {
            List<SourceRecordAndResult> results = new CopyOnWriteArrayList<>();
            processor.process(
                    currentRecords,
                    (SourceRecordAndResult recordAndResult) -> {
                        if (recordAndResult.error() != null) {
                            // some error occurred, early exit
                            finalStep.emit(
                                    new SourceRecordAndResult(
                                            initialSourceRecord, null, recordAndResult.error()));
                            return;
                        }

                        results.add(recordAndResult);

                        if (results.size() != currentRecords.size()) {
                            // we have to wait for each record to be processed
                            return;
                        }

                        List<Record> finalRecords = new ArrayList<>();
                        for (SourceRecordAndResult result : results) {
                            if (result.resultRecords() != null) {
                                finalRecords.addAll(result.resultRecords());
                            }
                        }
                        if (finalRecords.isEmpty()) {
                            processed(0, finalRecords.size());
                            finalStep.emit(
                                    new SourceRecordAndResult(
                                            initialSourceRecord, List.of(), null));
                        } else if (index == processors.size() - 1) {
                            // no more processors
                            processed(0, finalRecords.size());
                            finalStep.emit(
                                    new SourceRecordAndResult(
                                            initialSourceRecord, finalRecords, null));
                        } else {
                            // next processor
                            invokeProcessor(
                                    index + 1, finalRecords, initialSourceRecord, finalStep);
                        }
                    });
        } catch (Throwable error) {
            log.error("Internal Error processing record: {}", initialSourceRecord, error);
            finalStep.emit(new SourceRecordAndResult(initialSourceRecord, null, error));
        }
    }

    @Override
    public void process(List<Record> records, RecordSink sink) {
        if (records == null || records.isEmpty()) {
            throw new IllegalStateException("Records cannot be null or empty");
        }
        processed(records.size(), 0);
        if (processors.isEmpty()) {
            for (Record r : records) {
                sink.emit(new SourceRecordAndResult(r, List.of(r), null));
            }
            return;
        }
        for (Record record : records) {
            invokeProcessor(0, List.of(record), record, sink);
        }
    }

    @Override
    protected Map<String, Object> buildAdditionalInfo() {
        return Map.of();
    }

    @Override
    public final List<AgentStatusResponse> getAgentStatus() {
        List<AgentStatusResponse> result = new ArrayList<>();
        for (AgentProcessor processor : processors) {
            List<AgentStatusResponse> info = processor.getAgentStatus();
            result.addAll(info);
        }
        return result;
    }

    @Override
    public void restart() throws Exception {
        for (AgentProcessor processor : processors) {
            processor.restart();
        }
    }
}
