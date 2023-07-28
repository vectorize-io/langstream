package com.datastax.oss.sga.runtime.agent;

import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.api.runner.code.AgentCodeRegistry;
import com.datastax.oss.sga.api.runner.code.AgentContext;
import com.datastax.oss.sga.api.runner.code.AgentProcessor;
import com.datastax.oss.sga.api.runner.code.AgentSink;
import com.datastax.oss.sga.api.runner.code.AgentSource;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.topics.TopicAdmin;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntime;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntimeRegistry;
import com.datastax.oss.sga.api.runner.topics.TopicConsumer;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionProvider;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;
import com.datastax.oss.sga.runtime.agent.python.PythonCodeAgentProvider;
import com.datastax.oss.sga.runtime.agent.simple.IdentityAgentProvider;
import com.datastax.oss.sga.runtime.api.agent.RuntimePodConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * This is the main entry point for the pods that run the SGA runtime and Java code.
 */
@Slf4j
public class AgentRunner
{
    private static final TopicConnectionsRuntimeRegistry TOPIC_CONNECTIONS_REGISTRY = new TopicConnectionsRuntimeRegistry();
    private static final AgentCodeRegistry AGENT_CODE_REGISTRY = new AgentCodeRegistry();
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    private static MainErrorHandler mainErrorHandler = error -> {
        log.error("Unexpected error", error);
        System.exit(-1);
    };

    public interface MainErrorHandler {
        void handleError(Throwable error);
    }

    @SneakyThrows
    public static void main(String ... args) {
        try {
            if (args.length < 1) {
                throw new IllegalArgumentException("Missing pod configuration file argument");
            }
            Path podRuntimeConfiguration = Path.of(args[0]);
            log.info("Loading pod configuration from {}", podRuntimeConfiguration);

            RuntimePodConfiguration configuration = MAPPER.readValue(podRuntimeConfiguration.toFile(),
                    RuntimePodConfiguration.class);


            Path codeDirectory = Path.of(args[1]);
            log.info("Loading code from {}", codeDirectory);
            run(configuration, podRuntimeConfiguration, codeDirectory, -1);


        } catch (Throwable error) {
            log.info("Error, NOW SLEEPING", error);
            Thread.sleep(60000);
            mainErrorHandler.handleError(error);
        }
    }


    public static void run(RuntimePodConfiguration configuration,
                           Path podRuntimeConfiguration,
                           Path codeDirectory,
                           int maxLoops) throws Exception {
        log.info("Pod Configuration {}", configuration);

        // agentId is the identity of the agent in the cluster
        // it is shared by all the instances of the agent
        String agentId = configuration.agent().applicationId() + "-" + configuration.agent().agentId();

        log.info("Starting agent {} with configuration {}", agentId, configuration.agent());

        TopicConnectionsRuntime topicConnectionsRuntime =
                TOPIC_CONNECTIONS_REGISTRY.getTopicConnectionsRuntime(configuration.streamingCluster());

        log.info("TopicConnectionsRuntime {}", topicConnectionsRuntime);
        try {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                ClassLoader customLibClassloader = buildCustomLibClassloader(codeDirectory, contextClassLoader);
                Thread.currentThread().setContextClassLoader(customLibClassloader);
                AgentCode agentCode = initAgent(configuration);
                if (PythonCodeAgentProvider.isPythonCodeAgent(agentCode)) {
                    runPythonAgent(
                        podRuntimeConfiguration, codeDirectory);
                } else {
                    runJavaAgent(configuration, maxLoops, agentId, topicConnectionsRuntime, agentCode);
                }
            } finally {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }

        } finally {
            topicConnectionsRuntime.close();
        }
    }

    private static ClassLoader buildCustomLibClassloader(Path codeDirectory, ClassLoader contextClassLoader) throws IOException {
        ClassLoader customLibClassloader = contextClassLoader;
        if (codeDirectory == null) {
            return customLibClassloader;
        }
        Path javaLib = codeDirectory.resolve("java").resolve("lib");
        log.info("Looking for java lib in {}", javaLib);
        if (Files.exists(javaLib)&& Files.isDirectory(javaLib)) {
            List<URL> jars;
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(javaLib, "*.jar")) {
                jars = StreamSupport.stream(stream.spliterator(), false)
                        .map(p -> {
                            log.info("Adding jar {}", p);
                            try {
                                return p.toUri().toURL();
                            } catch (MalformedURLException e) {
                                log.error("Error adding jar {}", p, e);
                                // ignore
                                return null;
                            }
                        })
                        .filter(p -> p != null)
                        .collect(Collectors.toList());
                ;
            }
            customLibClassloader = new URLClassLoader(jars.toArray(URL[]::new), contextClassLoader);
        }
        return customLibClassloader;
    }

    private static void runPythonAgent(Path podRuntimeConfiguration, Path codeDirectory) throws Exception {

        Path pythonCodeDirectory = codeDirectory.resolve("python");
        log.info("Python code directory {}", pythonCodeDirectory);

        final String pythonPath = System.getenv("PYTHONPATH");
        final String newPythonPath = "%s:%s".formatted(pythonPath, pythonCodeDirectory.toAbsolutePath().toString());

        // copy input/output to standard input/output of the java process
        // this allows to use "kubectl logs" easily
        ProcessBuilder processBuilder = new ProcessBuilder(
                "python3", "-m", "sga_runtime", podRuntimeConfiguration.toAbsolutePath().toString())
                .inheritIO()
                .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                .redirectError(ProcessBuilder.Redirect.INHERIT);
        processBuilder
                .environment()
                .put("PYTHONPATH", newPythonPath);
        processBuilder
                .environment()
                .put("NLTK_DATA", "/app/nltk_data");
        Process process = processBuilder.start();

        int exitCode = process.waitFor();
        log.info("Python process exited with code {}", exitCode);

        if (exitCode != 0) {
            throw new Exception("Python code exited with code " + exitCode);
        }
    }

    private static void runJavaAgent(RuntimePodConfiguration configuration,
                                     int maxLoops,
                                     String agentId,
                                     TopicConnectionsRuntime topicConnectionsRuntime,
                                     AgentCode agentCode) throws Exception {
        topicConnectionsRuntime.init(configuration.streamingCluster());

        // this is closed by the TopicSource
        final TopicConsumer consumer;
        if (configuration.input() != null && !configuration.input().isEmpty()) {
            consumer = topicConnectionsRuntime.createConsumer(agentId,
                    configuration.streamingCluster(), configuration.input());
        } else {
            consumer = new NoopTopicConsumer();
        }

        // this is closed by the TopicSink
        final TopicProducer producer;
        if (configuration.output() != null && !configuration.output().isEmpty()) {
            producer = topicConnectionsRuntime.createProducer(agentId, configuration.streamingCluster(), configuration.output());
        } else {
            producer = new NoopTopicProducer();
        }

        ErrorsHandler errorsHandler = new StandardErrorsHandler(configuration.agent().errorHandlerConfiguration());

        TopicAdmin topicAdmin = topicConnectionsRuntime.createTopicAdmin(agentId,
                configuration.streamingCluster(),
                configuration.output());

        try {

            AgentProcessor mainProcessor;
            if (agentCode instanceof AgentProcessor agentProcessor) {
                mainProcessor = agentProcessor;
            } else {
                mainProcessor = new IdentityAgentProvider.IdentityAgentCode();
            }

            AgentSource source = null;
            if (agentCode instanceof AgentSource agentSource) {
                source = agentSource;
            } else if (agentCode instanceof CompositeAgentProcessor compositeAgentProcessor) {
                source = compositeAgentProcessor.getSource();
            }
            if (source == null) {
                source = new TopicConsumerSource(consumer);
            }

            AgentSink sink = null;
            if (agentCode instanceof AgentSink agentSink) {
                sink = agentSink;
            } else if (agentCode instanceof CompositeAgentProcessor compositeAgentProcessor) {
                sink = compositeAgentProcessor.getSink();
            }

            if (sink == null) {
                sink = new TopicProducerSink(producer);
            }

            topicAdmin.start();
            AgentContext agentContext = new SimpleAgentContext(agentId, consumer, producer, topicAdmin,
                    new TopicConnectionProvider() {
                        @Override
                        public TopicConsumer createConsumer(String agentId, Map<String, Object> config) {
                            return topicConnectionsRuntime.createConsumer(agentId, configuration.streamingCluster(), config);
                        }

                        @Override
                        public TopicProducer createProducer(String agentId, Map<String, Object> config) {
                            return topicConnectionsRuntime.createProducer(agentId, configuration.streamingCluster(), config);
                        }
                    });
            log.info("Source: {}", source);
            log.info("Processor: {}", mainProcessor);
            log.info("Sink: {}", sink);

            runMainLoop(source, mainProcessor, sink, agentContext, errorsHandler, maxLoops);
        } finally {
            topicAdmin.close();
        }
    }

    static void runMainLoop(AgentSource source,
                                    AgentProcessor function,
                                    AgentSink sink,
                                    AgentContext agentContext,
                                    ErrorsHandler errorsHandler,
                                    int maxLoops) throws Exception {
        try {
            source.setContext(agentContext);
            sink.setContext(agentContext);
            function.setContext(agentContext);
            source.start();
            sink.start();
            function.start();

            SourceRecordTracker sourceRecordTracker =
                    new SourceRecordTracker(source);
            sink.setCommitCallback(sourceRecordTracker);

            List<Record> records = source.read();
            while ((maxLoops < 0) || (maxLoops-- > 0)) {
                if (records != null && !records.isEmpty()) {
                    // in case of permanent FAIL this method will throw an exception
                    List<AgentProcessor.SourceRecordAndResult> sinkRecords
                            = runProcessorAgent(function, records, errorsHandler, source);
                    // sinkRecord == null is the SKIP case

                    // in this case we do not send the records to the sink
                    // and the source has already committed the records
                    // This won't for the Kafka Connect Sink, because it does handle the commit
                    // itself, but on the other hand in the case of the Connect Sink we can see here
                    // only the IdentityAgentCode that is not supposed to fail

                    if (sinkRecords != null) {
                        try {
                            // the function maps the record coming from the Source to records to be sent to the Sink
                            sourceRecordTracker.track(sinkRecords);
                            for (AgentProcessor.SourceRecordAndResult sourceRecordAndResult : sinkRecords) {
                                if (sourceRecordAndResult.getError() != null) {
                                    // commit skipped records
                                    source.commit(List.of(sourceRecordAndResult.getSourceRecord()));
                                } else {
                                    List<Record> forTheSink = new ArrayList<>();
                                    forTheSink.addAll(sourceRecordAndResult.getResultRecords());
                                    sink.write(forTheSink);
                                }
                            }
                        } catch (Throwable e) {
                            log.error("Error while processing records", e);

                            // throw the error
                            // this way the consumer will not commit the records
                            throw new RuntimeException("Error while processing records", e);
                        }
                    }
                }

                // commit (Kafka Connect Sink)
                if (sink.handlesCommit()) {
                    // this is the case for the Kafka Connect Sink
                    // in this case it handles directly the Kafka Consumer
                    // and so we bypass the commit
                    sink.commit();
                }

                records = source.read();
            }
        } finally {
            function.close();
            source.close();
            sink.close();
        }
    }


    private static List<AgentProcessor.SourceRecordAndResult> runProcessorAgent(AgentProcessor processor,
                                                                                List<Record> sourceRecords,
                                                                                ErrorsHandler errorsHandler,
                                                                                AgentSource source) throws Exception {
        List<Record> recordToProcess = sourceRecords;
        Map<Record, AgentProcessor.SourceRecordAndResult> resultsByRecord = new HashMap<>();
        int trialNumber = 0;
        while (!recordToProcess.isEmpty()) {
            log.info("runProcessor on {} records (trial #{})", recordToProcess.size(), trialNumber++);
            List<AgentProcessor.SourceRecordAndResult> results = safeProcessRecords(processor, recordToProcess);

            recordToProcess = new ArrayList<>();
            List<AgentProcessor.SourceRecordAndResult> sinkRecords = new ArrayList<>();
            for (AgentProcessor.SourceRecordAndResult result : results) {
                Record sourceRecord = result.getSourceRecord();
                resultsByRecord.put(sourceRecord, result);
                if (result.getError() != null) {
                    Throwable error = result.getError();
                    // handle error
                    ErrorsHandler.ErrorsProcessingOutcome action = errorsHandler.handleErrors(sourceRecord, result.getError());
                    switch (action) {
                        case SKIP:
                            log.error("Unrecoverable error while processing the records, skipping", error);
                            resultsByRecord.put(sourceRecord,
                                    new AgentProcessor.SourceRecordAndResult(sourceRecord, List.of(), error));
                            break;
                        case RETRY:
                            log.error("Retryable error while processing the records, retrying", error);
                            // retry
                            recordToProcess.add(sourceRecord);
                            break;
                        case FAIL:
                            log.error("Unrecoverable error while processing some the records, failing", error);
                            // fail
                            throw new PermanentFailureException(error);
                    }
                }
            }
        }

        List<AgentProcessor.SourceRecordAndResult> finalResult = new ArrayList<>(sourceRecords.size());
        for (Record sourceRecord : sourceRecords) {
            finalResult.add(resultsByRecord.get(sourceRecord));
        }
        return finalResult;
    }

    private static List<AgentProcessor.SourceRecordAndResult> safeProcessRecords(AgentProcessor processor, List<Record> recordToProcess) {
        List<AgentProcessor.SourceRecordAndResult> results;
        try {
            results =  processor.process(recordToProcess);
        } catch (Throwable error) {
            results = recordToProcess
                    .stream()
                    .map(r -> new AgentProcessor.SourceRecordAndResult(r, null, error))
                    .toList();
        }
        return results;
    }

    public static final class PermanentFailureException extends Exception {
        public PermanentFailureException(Throwable cause) {
            super(cause);
        }
    }

    private static AgentCode initAgent(RuntimePodConfiguration configuration) throws Exception {
        log.info("Bootstrapping agent with configuration {}", configuration.agent());
        return initAgent(configuration.agent().agentType(), configuration.agent().configuration());
    }

    public static AgentCode initAgent(String agentType, Map<String, Object> configuration) throws Exception {
        AgentCode agentCode = AGENT_CODE_REGISTRY.getAgentCode(agentType);
        agentCode.init(configuration);
        return agentCode;
    }

    public static MainErrorHandler getErrorHandler() {
        return mainErrorHandler;
    }

    public static void setErrorHandler(MainErrorHandler mainErrorHandler) {
        AgentRunner.mainErrorHandler = mainErrorHandler;
    }

    private static class NoopTopicConsumer implements TopicConsumer {
        @SneakyThrows

        @Override
        public List<Record> read() {
            log.info("Sleeping for 1 second, no records...");
            Thread.sleep(1000);
            return List.of();
        }
    }

    private static class NoopTopicProducer implements TopicProducer {
    }

    private static class SimpleAgentContext implements AgentContext {
        private final TopicConsumer consumer;
        private final TopicProducer producer;
        private final TopicAdmin topicAdmin;
        private final String agentId;

        private final TopicConnectionProvider topicConnectionProvider;

        public SimpleAgentContext(String agentId, TopicConsumer consumer, TopicProducer producer, TopicAdmin topicAdmin,
                                  TopicConnectionProvider topicConnectionProvider) {
            this.consumer = consumer;
            this.producer = producer;
            this.topicAdmin = topicAdmin;
            this.agentId = agentId;
            this.topicConnectionProvider = topicConnectionProvider;
        }

        @Override
        public String getAgentId() {
            return agentId;
        }

        @Override
        public TopicConsumer getTopicConsumer() {
            return consumer;
        }

        @Override
        public TopicProducer getTopicProducer() {
            return producer;
        }

        @Override
        public TopicAdmin getTopicAdmin() {
            return topicAdmin;
        }

        @Override
        public TopicConnectionProvider getTopicConnectionProvider() {
            return topicConnectionProvider;
        }
    }

}
