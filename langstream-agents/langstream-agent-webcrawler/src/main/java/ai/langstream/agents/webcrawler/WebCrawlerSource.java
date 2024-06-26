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

import ai.langstream.agents.webcrawler.crawler.Document;
import ai.langstream.agents.webcrawler.crawler.StatusStorage;
import ai.langstream.agents.webcrawler.crawler.WebCrawler;
import ai.langstream.agents.webcrawler.crawler.WebCrawlerConfiguration;
import ai.langstream.agents.webcrawler.crawler.WebCrawlerStatus;
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
import io.minio.*;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WebCrawlerSource extends AbstractAgentCode implements AgentSource {

    private int maxUnflushedPages = 100;

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

    private WebCrawler crawler;

    private boolean finished;

    private final AtomicInteger flushNext = new AtomicInteger(100);

    private final BlockingQueue<Document> foundDocuments = new LinkedBlockingQueue<>();

    @Getter private StateStorage<StatusStorage.Status> stateStorage;

    private Runnable onReindexStart;

    private TopicProducer deletedDocumentsProducer;

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

        log.info("allowed-domains: {}", allowedDomains);
        log.info("forbidden-paths: {}", forbiddenPaths);
        log.info("allow-non-html-contents: {}", allowNonHtmlContents);
        log.info("seed-urls: {}", seedUrls);
        log.info("max-urls: {}", maxUrls);
        log.info("max-depth: {}", maxDepth);
        log.info("handle-robots-file: {}", handleRobotsFile);
        log.info("scan-html-documents: {}", scanHtmlDocuments);
        log.info("user-agent: {}", userAgent);
        log.info("max-unflushed-pages: {}", maxUnflushedPages);
        log.info("min-time-between-requests: {}", minTimeBetweenRequests);
        log.info("reindex-interval-seconds: {}", reindexIntervalSeconds);

        WebCrawlerConfiguration webCrawlerConfiguration =
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
                        .build();

        WebCrawlerStatus status = new WebCrawlerStatus();
        // this can be overwritten when the status is reloaded
        status.setLastIndexStartTimestamp(System.currentTimeMillis());
        crawler =
                new WebCrawler(
                        webCrawlerConfiguration,
                        status,
                        foundDocuments::add,
                        this::sendDeletedDocument);
    }

    private void sendDeletedDocument(String url) throws Exception {
        if (deletedDocumentsProducer != null) {
            SimpleRecord simpleRecord =
                    SimpleRecord.builder().headers(sourceRecordHeaders).value(url).build();
            // sync so we can handle status correctly
            deletedDocumentsProducer.write(simpleRecord).get();
        }
    }

    @Override
    public void setContext(AgentContext context) throws Exception {
        super.setContext(context);
        bucketName = getString("bucketName", "langstream-source", agentConfiguration);
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
                // we did something but no new documents were found (for instance a redirection has
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
            if (onReindexStart != null) {
                // for tests
                onReindexStart.run();
            }
            log.info(
                    "Reindexing is needed, last index end timestamp is {}, {} seconds ago",
                    Instant.ofEpochMilli(lastIndexEndTimestamp),
                    elapsedSeconds);
            crawler.restartIndexing(seedUrls);
            finished = false;
            flushStatus();
        } else {
            log.debug(
                    "Reindexing is not needed, last end start timestamp is {}, {} seconds ago",
                    Instant.ofEpochMilli(lastIndexEndTimestamp),
                    elapsedSeconds);
        }
    }

    private List<Record> sleepForNoResults() throws Exception {
        Thread.sleep(100);
        return List.of();
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
        for (Record record : records) {
            String url = (String) record.key();
            crawler.getStatus().urlProcessed(url);

            if (flushNext.decrementAndGet() == 0) {
                flushStatus();
                flushNext.set(maxUnflushedPages);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (deletedDocumentsProducer != null) {
            deletedDocumentsProducer.close();
        }
        if (stateStorage != null) {
            stateStorage.close();
        }
    }

    @Override
    public void cleanup(Map<String, Object> configuration, AgentContext context) throws Exception {
        super.cleanup(configuration, context);
        String bucketName = getString("bucketName", "langstream-source", agentConfiguration);
        try (StateStorage<StatusStorage.Status> statusStateStorage =
                initStateStorage(agentId(), context, agentConfiguration, bucketName); ) {
            if (statusStateStorage != null) {
                statusStateStorage.delete();
            }
        }
    }
}
