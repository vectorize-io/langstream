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
package ai.langstream.agents.webcrawler;

import static ai.langstream.agents.webcrawler.crawler.WebCrawlerConfiguration.DEFAULT_USER_AGENT;
import static ai.langstream.api.util.ConfigurationUtils.*;

import ai.langstream.agents.webcrawler.crawler.*;
import ai.langstream.ai.agents.commons.state.LocalDiskStateStorage;
import ai.langstream.ai.agents.commons.state.S3StateStorage;
import ai.langstream.ai.agents.commons.state.StateStorage;
import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.util.ObjectMapperFactory;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.minio.*;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WebCrawlerSource extends AbstractAgentCode implements AgentSource {

    private int maxUnflushedPages;

    private String bucketName;
    private Set<String> allowedDomains;
    private Set<String> forbiddenPaths;
    private int maxUrls;
    private boolean handleRobotsFile;
    private boolean scanHtmlDocuments;
    private Set<String> seedUrls;
    private Map<String, Object> agentConfiguration;
    private int reindexIntervalSeconds;
    private Collection<Header> sourceRecordHeaders;

    private String sourceActivitySummaryTopic;
    private TopicProducer sourceActivitySummaryProducer;

    private List<String> sourceActivitySummaryEvents;

    private int sourceActivitySummaryNumEventsThreshold;
    private int sourceActivitySummaryTimeSecondsThreshold;

    private WebCrawler crawler;

    private boolean finished;

    private final AtomicInteger flushNext = new AtomicInteger(100);

    private final BlockingQueue<Document> foundDocuments = new LinkedBlockingQueue<>();

    @Getter private StateStorage<StatusStorage.Status> stateStorage;

    private Runnable onReindexStart;

    private TopicProducer deletedDocumentsProducer;

    public record ObjectDetail(String object, long detectedAt) {}

    @Getter
    @AllArgsConstructor
    public class SourceActivitySummaryWithCounts {
        @JsonProperty("newObjects")
        private List<ObjectDetail> newObjects;

        @JsonProperty("updatedObjects")
        private List<ObjectDetail> updatedObjects;

        @JsonProperty("unchangedObjects")
        private List<ObjectDetail> unchangedObjects;

        @JsonProperty("deletedObjects")
        private List<ObjectDetail> deletedObjects;

        @JsonProperty("newObjectsCount")
        private int newObjectsCount;

        @JsonProperty("updatedObjectsCount")
        private int changedObjectsCount;

        @JsonProperty("deletedObjectsCount")
        private int deletedObjectsCount;
    }

    private List<ObjectDetail> convertToObjectDetail(
            List<StatusStorage.UrlActivityDetail> urlActivityDetails) {
        return urlActivityDetails.stream()
                .map(detail -> new ObjectDetail(detail.url(), detail.detectedAt()))
                .collect(Collectors.toList());
    }

    public Runnable getOnReindexStart() {
        return onReindexStart;
    }

    public void setOnReindexStart(Runnable onReindexStart) {
        this.onReindexStart = onReindexStart;
    }

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        agentConfiguration = configuration;

        allowedDomains = getSet("allowed-domains", configuration);
        forbiddenPaths = getSet("forbidden-paths", configuration);
        maxUrls = getInt("max-urls", 1000, configuration);
        int maxDepth = getInt("max-depth", 50, configuration);
        handleRobotsFile = getBoolean("handle-robots-file", true, configuration);
        scanHtmlDocuments = getBoolean("scan-html-documents", true, configuration);
        seedUrls = getSet("seed-urls", configuration);
        reindexIntervalSeconds = getInt("reindex-interval-seconds", 60 * 60 * 24, configuration);
        maxUnflushedPages = getInt("max-unflushed-pages", 100, configuration);

        flushNext.set(maxUnflushedPages);
        final int minTimeBetweenRequests = getInt("min-time-between-requests", 500, configuration);
        final String userAgent = getString("user-agent", DEFAULT_USER_AGENT, configuration);
        final int maxErrorCount = getInt("max-error-count", 5, configuration);
        final int httpTimeout = getInt("http-timeout", 10000, configuration);
        final boolean allowNonHtmlContents =
                getBoolean("allow-non-html-contents", false, configuration);

        final boolean handleCookies = getBoolean("handle-cookies", true, configuration);
        sourceRecordHeaders =
                getMap("source-record-headers", Map.of(), configuration).entrySet().stream()
                        .map(
                                entry ->
                                        SimpleRecord.SimpleHeader.of(
                                                entry.getKey(), entry.getValue()))
                        .collect(Collectors.toUnmodifiableList());

        final boolean onlyMainContent = getBoolean("only-main-content", false, configuration);
        final Set<String> excludeFromMainContentTags =
                getSet("exclude-from-main-content-tags", configuration);

        WebCrawlerConfiguration.WebCrawlerConfigurationBuilder builder =
                WebCrawlerConfiguration.builder()
                        .allowedDomains(allowedDomains)
                        .allowNonHtmlContents(allowNonHtmlContents)
                        .maxUrls(maxUrls)
                        .maxDepth(maxDepth)
                        .forbiddenPaths(forbiddenPaths)
                        .handleRobotsFile(handleRobotsFile)
                        .minTimeBetweenRequests(minTimeBetweenRequests)
                        .userAgent(userAgent)
                        .handleCookies(handleCookies)
                        .httpTimeout(httpTimeout)
                        .maxErrorCount(maxErrorCount)
                        .onlyMainContent(onlyMainContent);
        if (!excludeFromMainContentTags.isEmpty()) {
            builder.excludeFromMainContentTags(excludeFromMainContentTags);
        }
        WebCrawlerConfiguration webCrawlerConfiguration = builder.build();
        log.info("configuration: {}", webCrawlerConfiguration);

        WebCrawlerStatus status = new WebCrawlerStatus();
        // this can be overwritten when the status is reloaded
        status.setLastIndexStartTimestamp(System.currentTimeMillis());

        final List<String> emitContentDiff =
                getList("emit-content-diff", configuration).stream()
                        .map(String::toLowerCase)
                        .toList();

        crawler =
                new WebCrawler(
                        webCrawlerConfiguration,
                        status,
                        new DocumentVisitor() {
                            @Override
                            public void visit(Document document) {
                                if (document.contentDiff() == null
                                        || emitContentDiff.isEmpty()
                                        || emitContentDiff.contains(
                                                document.contentDiff().toString().toLowerCase())) {
                                    foundDocuments.add(document);
                                } else {
                                    log.info(
                                            "Discarding document with content diff {}",
                                            document.contentDiff());
                                }
                            }
                        },
                        this::sendDeletedDocument);

        sourceActivitySummaryTopic =
                getString("source-activity-summary-topic", null, configuration);
        sourceActivitySummaryEvents = getList("source-activity-summary-events", configuration);
        sourceActivitySummaryNumEventsThreshold =
                getInt("source-activity-summary-events-threshold", 0, configuration);
        sourceActivitySummaryTimeSecondsThreshold =
                getInt("source-activity-summary-time-seconds-threshold", 30, configuration);
        if (sourceActivitySummaryTimeSecondsThreshold < 0) {
            throw new IllegalArgumentException(
                    "source-activity-summary-time-seconds-threshold must be > 0");
        }
    }

    private void sendDeletedDocument(String url) throws Exception {
        if (deletedDocumentsProducer != null) {
            // Add record type to headers
            List<Header> allHeaders = new ArrayList<>(sourceRecordHeaders);
            allHeaders.add(new SimpleRecord.SimpleHeader("recordType", "sourceObjectDeleted"));
            allHeaders.add(new SimpleRecord.SimpleHeader("recordSource", "webcrawler"));
            SimpleRecord simpleRecord =
                    SimpleRecord.builder().headers(allHeaders).value(url).build();
            // sync so we can handle status correctly
            deletedDocumentsProducer.write(simpleRecord).get();
        }
    }

    @Override
    public void setContext(AgentContext context) throws Exception {
        super.setContext(context);
        bucketName = getBucketNameFromConfig(agentConfiguration);
        stateStorage = initStateStorage(agentId(), context, agentConfiguration, bucketName);

        final String deletedDocumentsTopic =
                getString("deleted-documents-topic", null, agentConfiguration);
        if (deletedDocumentsTopic != null) {
            deletedDocumentsProducer =
                    agentContext
                            .getTopicConnectionProvider()
                            .createProducer(
                                    agentContext.getGlobalAgentId(),
                                    deletedDocumentsTopic,
                                    Map.of());
            deletedDocumentsProducer.start();
        }
        if (sourceActivitySummaryTopic != null) {
            sourceActivitySummaryProducer =
                    agentContext
                            .getTopicConnectionProvider()
                            .createProducer(
                                    agentContext.getGlobalAgentId(),
                                    sourceActivitySummaryTopic,
                                    Map.of());
            sourceActivitySummaryProducer.start();
        }
    }

    private static StateStorage<StatusStorage.Status> initStateStorage(
            final String agentId,
            AgentContext context,
            Map<String, Object> agentConfiguration,
            String bucketName) {
        final String globalAgentId = context.getGlobalAgentId();
        final String stateStorage = getString("state-storage", "s3", agentConfiguration);

        if (stateStorage.equals("disk")) {
            Optional<Path> localDiskPath = context.getPersistentStateDirectoryForAgent(agentId);
            if (!localDiskPath.isPresent()) {
                throw new IllegalArgumentException(
                        "No local disk path available for agent "
                                + agentId
                                + " and state-storage was set to 'disk'");
            }
            log.info("Using local disk storage");
            final Path statusFilename =
                    LocalDiskStateStorage.computePath(
                            localDiskPath,
                            context.getTenant(),
                            globalAgentId,
                            agentConfiguration,
                            "webcrawler");
            log.info("Status file is {}", statusFilename);
            return new LocalDiskStateStorage<>(statusFilename);
        } else {
            log.info("Using S3 storage");
            // since these config values are different we can't use StateStorageProvider
            String endpoint =
                    getString(
                            "endpoint", "http://minio-endpoint.-not-set:9090", agentConfiguration);
            String username = getString("access-key", "minioadmin", agentConfiguration);
            String password = getString("secret-key", "minioadmin", agentConfiguration);
            String region = getString("region", "", agentConfiguration);

            log.info(
                    "Connecting to S3 Bucket at {} in region {} with user {}",
                    endpoint,
                    region,
                    username);

            MinioClient.Builder builder =
                    MinioClient.builder().endpoint(endpoint).credentials(username, password);
            if (!region.isBlank()) {
                builder.region(region);
            }
            MinioClient minioClient = builder.build();
            String statusFileName =
                    S3StateStorage.computeObjectName(
                            context.getTenant(), globalAgentId, agentConfiguration, "webcrawler");
            log.info("Status file is {}", statusFileName);
            return new S3StateStorage<>(minioClient, bucketName, statusFileName);
        }
    }

    public WebCrawler getCrawler() {
        return crawler;
    }

    private void flushStatus() {
        try {
            crawler.getStatus().persist(stateStorage);
        } catch (Exception e) {
            log.error("Error persisting status", e);
        }
    }

    @Override
    public void start() throws Exception {
        crawler.reloadStatus(stateStorage);

        for (String url : seedUrls) {
            crawler.crawl(url);
        }
    }

    @Override
    public List<Record> read() throws Exception {
        synchronized (this) {
            sendSourceActivitySummaryIfNeeded();
            if (finished) {
                checkReindexIsNeeded();
                return sleepForNoResults();
            }
            if (foundDocuments.isEmpty()) {
                boolean somethingDone = crawler.runCycle();
                if (!somethingDone) {
                    finished = true;
                    log.info("No more documents found, checking deleted documents");
                    try {
                        crawler.runDeletedDocumentsChecker();
                    } catch (Throwable tt) {
                        log.error("Error checking deleted documents", tt);
                    }
                    log.info("No more documents to check");
                    crawler.getStatus().setLastIndexEndTimestamp(System.currentTimeMillis());
                    if (reindexIntervalSeconds > 0) {
                        Instant next =
                                Instant.ofEpochMilli(crawler.getStatus().getLastIndexEndTimestamp())
                                        .plusSeconds(reindexIntervalSeconds);
                        log.info(
                                "Next re-index will happen in {} seconds, at {}",
                                reindexIntervalSeconds,
                                next);
                    }
                    flushStatus();
                } else {
                    // we did something but no new documents were found (for instance a redirection
                    // has
                    // been processed)
                    // no need to sleep
                    if (foundDocuments.isEmpty()) {
                        log.info("The last cycle didn't produce any new documents");
                        return List.of();
                    }
                }
            }
            if (foundDocuments.isEmpty()) {
                return sleepForNoResults();
            }

            Document document = foundDocuments.remove();
            processed(0, 1);

            List<Header> allHeaders = new ArrayList<>(sourceRecordHeaders);
            allHeaders.add(new SimpleRecord.SimpleHeader("url", document.url()));
            allHeaders.add(new SimpleRecord.SimpleHeader("content_type", document.contentType()));
            allHeaders.add(
                    new SimpleRecord.SimpleHeader(
                            "content_diff", document.contentDiff().toString().toLowerCase()));
            SimpleRecord simpleRecord =
                    SimpleRecord.builder()
                            .headers(allHeaders)
                            .key(document.url())
                            .value(document.content())
                            .build();

            return List.of(simpleRecord);
        }
    }

    private void checkReindexIsNeeded() {
        if (reindexIntervalSeconds <= 0) {
            return;
        }
        long lastIndexEndTimestamp = crawler.getStatus().getLastIndexEndTimestamp();
        if (lastIndexEndTimestamp <= 0) {
            // indexing is not finished yet
            log.debug("Reindexing is not needed, indexing is not finished yet");
            return;
        }
        long now = System.currentTimeMillis();
        long elapsedSeconds = (now - lastIndexEndTimestamp) / 1000;
        if (elapsedSeconds >= reindexIntervalSeconds) {

            log.info(
                    "Reindexing is needed, last index end timestamp is {}, {} seconds ago",
                    Instant.ofEpochMilli(lastIndexEndTimestamp),
                    elapsedSeconds);
            reindex();
        } else {
            log.debug(
                    "Reindexing is not needed, last end start timestamp is {}, {} seconds ago",
                    Instant.ofEpochMilli(lastIndexEndTimestamp),
                    elapsedSeconds);
        }
    }

    private void reindex() {
        if (onReindexStart != null) {
            // for tests
            onReindexStart.run();
        }
        crawler.restartIndexing(seedUrls);
        finished = false;
        flushStatus();
    }

    private List<Record> sleepForNoResults() throws Exception {
        Thread.sleep(100);
        return List.of();
    }

    private void sendSourceActivitySummaryIfNeeded() throws Exception {
        StatusStorage.SourceActivitySummary currentSourceActivitySummary =
                crawler.getStatus().getCurrentSourceActivitySummary();
        if (currentSourceActivitySummary == null) {
            return;
        }
        int countEvents = 0;
        long firstEventTs = Long.MAX_VALUE;
        if (sourceActivitySummaryEvents.contains("new")) {
            countEvents += currentSourceActivitySummary.newUrls().size();
            firstEventTs =
                    currentSourceActivitySummary.newUrls().stream()
                            .mapToLong(StatusStorage.UrlActivityDetail::detectedAt)
                            .min()
                            .orElse(Long.MAX_VALUE);
        }
        if (sourceActivitySummaryEvents.contains("changed")) {
            countEvents += currentSourceActivitySummary.changedUrls().size();
            firstEventTs =
                    Math.min(
                            firstEventTs,
                            currentSourceActivitySummary.changedUrls().stream()
                                    .mapToLong(StatusStorage.UrlActivityDetail::detectedAt)
                                    .min()
                                    .orElse(Long.MAX_VALUE));
        }
        if (sourceActivitySummaryEvents.contains("unchanged")) {
            countEvents += currentSourceActivitySummary.unchangedUrls().size();
            firstEventTs =
                    Math.min(
                            firstEventTs,
                            currentSourceActivitySummary.unchangedUrls().stream()
                                    .mapToLong(StatusStorage.UrlActivityDetail::detectedAt)
                                    .min()
                                    .orElse(Long.MAX_VALUE));
        }
        if (sourceActivitySummaryEvents.contains("deleted")) {
            countEvents += currentSourceActivitySummary.deletedUrls().size();
            firstEventTs =
                    Math.min(
                            firstEventTs,
                            currentSourceActivitySummary.deletedUrls().stream()
                                    .mapToLong(StatusStorage.UrlActivityDetail::detectedAt)
                                    .min()
                                    .orElse(Long.MAX_VALUE));
        }
        if (countEvents == 0) {
            return;
        }
        long now = System.currentTimeMillis();

        boolean emit = false;
        boolean isTimeForStartSummaryOver =
                now >= firstEventTs + sourceActivitySummaryTimeSecondsThreshold * 1000L;
        if (!isTimeForStartSummaryOver) {
            // no time yet, but we have enough events to send
            if (sourceActivitySummaryNumEventsThreshold > 0
                    && countEvents >= sourceActivitySummaryNumEventsThreshold) {
                log.info(
                        "Emitting source activity summary, events {} with threshold of {}",
                        countEvents,
                        sourceActivitySummaryNumEventsThreshold);
                emit = true;
            }
        } else {
            log.info(
                    "Emitting source activity summary due to time threshold (first event was {} seconds ago)",
                    (now - firstEventTs) / 1000);
            // time is over, we should send summary
            emit = true;
        }
        if (emit) {
            if (sourceActivitySummaryProducer != null) {
                log.info(
                        "Emitting source activity summary to topic {}", sourceActivitySummaryTopic);
                // Create a new SourceActivitySummaryWithCounts object directly
                SourceActivitySummaryWithCounts summaryWithCounts =
                        new SourceActivitySummaryWithCounts(
                                convertToObjectDetail(currentSourceActivitySummary.newUrls()),
                                convertToObjectDetail(currentSourceActivitySummary.changedUrls()),
                                convertToObjectDetail(currentSourceActivitySummary.unchangedUrls()),
                                convertToObjectDetail(currentSourceActivitySummary.deletedUrls()),
                                currentSourceActivitySummary.newUrls().size(),
                                currentSourceActivitySummary.changedUrls().size(),
                                currentSourceActivitySummary.deletedUrls().size());

                // Convert the new object to JSON
                String value = ObjectMapperFactory.getDefaultMapper().writeValueAsString(summaryWithCounts);
                List<Header> allHeaders = new ArrayList<>(sourceRecordHeaders);
                allHeaders.add(
                        new SimpleRecord.SimpleHeader("recordType", "sourceActivitySummary"));
                allHeaders.add(new SimpleRecord.SimpleHeader("recordSource", "webcrawler"));
                SimpleRecord simpleRecord =
                        SimpleRecord.builder().headers(allHeaders).value(value).build();
                sourceActivitySummaryProducer.write(simpleRecord).get();
            } else {
                log.warn("No source activity summary producer configured, event will be lost");
            }
            crawler.getStatus()
                    .setCurrentSourceActivitySummary(
                            new StatusStorage.SourceActivitySummary(
                                    new ArrayList<>(),
                                    new ArrayList<>(),
                                    new ArrayList<>(),
                                    new ArrayList<>()));
            crawler.getStatus().persist(stateStorage);
        }
    }

    @Override
    protected Map<String, Object> buildAdditionalInfo() {
        Map<String, Object> additionalInfo = new HashMap<>();
        additionalInfo.put("seed-Urls", seedUrls);
        additionalInfo.put("allowed-domains", allowedDomains);
        additionalInfo.put("statusFileName", stateStorage.getStateReference());
        additionalInfo.put("bucketName", bucketName);
        return additionalInfo;
    }

    @Override
    public void commit(List<Record> records) {
        synchronized (this) {
            for (Record record : records) {
                String url = (String) record.key();
                Document.ContentDiff contentDiff =
                        Document.ContentDiff.valueOf(
                                record.getHeader("content_diff").valueAsString().toUpperCase());
                crawler.getStatus().urlProcessed(url, contentDiff);

                if (flushNext.decrementAndGet() == 0) {
                    flushStatus();
                    flushNext.set(maxUnflushedPages);
                }
            }
        }
    }

    @Override
    public void onSignal(Record record) throws Exception {
        Object key = record.key();
        if (key == null) {
            log.warn("skipping signal with null key {}", record);
            return;
        }
        synchronized (this) {
            switch (key.toString()) {
                case "invalidate-all":
                    log.info("Invaliding all, triggering reindex");
                    foundDocuments.clear();
                    flushNext.set(100);
                    getCrawler().getStatus().reset();
                    reindex();
                    break;
                default:
                    log.warn("Unknown signal key {}", key);
                    break;
            }
        }
    }

    @Override
    public void close() {
        super.close();
        if (deletedDocumentsProducer != null) {
            deletedDocumentsProducer.close();
        }
        if (sourceActivitySummaryProducer != null) {
            sourceActivitySummaryProducer.close();
        }
        if (stateStorage != null) {
            try {
                stateStorage.close();
            } catch (Exception e) {
                log.error("Error closing state storage", e);
            }
        }
    }

    @Override
    public void cleanup(Map<String, Object> configuration, AgentContext context) throws Exception {
        super.cleanup(configuration, context);
        String bucketName = getBucketNameFromConfig(configuration);
        try (StateStorage<StatusStorage.Status> statusStateStorage =
                initStateStorage(agentId(), context, configuration, bucketName); ) {
            statusStateStorage.delete();
        }
    }

    private static String getBucketNameFromConfig(Map<String, Object> configuration) {
        return getString("bucketName", "langstream-source", configuration);
    }
}
