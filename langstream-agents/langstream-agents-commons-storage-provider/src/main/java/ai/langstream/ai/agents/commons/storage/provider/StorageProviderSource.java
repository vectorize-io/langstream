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
package ai.langstream.ai.agents.commons.storage.provider;

import ai.langstream.ai.agents.commons.state.StateStorage;
import ai.langstream.ai.agents.commons.state.StateStorageProvider;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderSourceState.ObjectDetail;
import ai.langstream.api.runner.code.*;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.TopicProducer;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class StorageProviderSource<T extends StorageProviderSourceState>
        extends AbstractAgentCode implements AgentSource {

    public static final ObjectMapper MAPPER = new ObjectMapper();
    private Map<String, Object> agentConfiguration;
    private final Set<String> objectsToCommit = ConcurrentHashMap.newKeySet();
    @Getter private StateStorage<T> stateStorage;

    private TopicProducer deletedObjectsProducer;
    private TopicProducer sourceActivitySummaryProducer;

    public abstract Class<T> getStateClass();

    public abstract void initializeClientAndConfig(Map<String, Object> configuration);

    public abstract String getBucketName();

    public abstract boolean isDeleteObjects();

    public abstract int getIdleTime();

    public abstract String getDeletedObjectsTopic();

    public abstract String getSourceActivitySummaryTopic();

    public abstract List<String> getSourceActivitySummaryEvents();

    public abstract int getSourceActivitySummaryNumEventsThreshold();

    public abstract int getSourceActivitySummaryTimeSecondsThreshold();

    public abstract Collection<StorageProviderObjectReference> listObjects() throws Exception;

    public abstract byte[] downloadObject(StorageProviderObjectReference object) throws Exception;

    public abstract void deleteObject(String id) throws Exception;

    public abstract Collection<Header> getSourceRecordHeaders();

    public abstract boolean isStateStorageRequired();

    @Getter
    @AllArgsConstructor
    public class SourceActivitySummaryWithCounts {
        @JsonProperty("newObjects")
        private List<ObjectDetail> newObjects;

        @JsonProperty("updatedObjects")
        private List<ObjectDetail> updatedObjects;

        @JsonProperty("deletedObjects")
        private List<ObjectDetail> deletedObjects;

        @JsonProperty("newObjectsCount")
        private int newObjectsCount;

        @JsonProperty("updatedObjectsCount")
        private int updatedObjectsCount;

        @JsonProperty("deletedObjectsCount")
        private int deletedObjectsCount;
    }

    @Override
    public void init(Map<String, Object> configuration) {
        agentConfiguration = configuration;
        initializeClientAndConfig(configuration);
    }

    @Override
    public void setContext(AgentContext context) throws Exception {
        super.setContext(context);
        stateStorage =
                new StateStorageProvider<T>()
                        .create(
                                context.getTenant(),
                                agentId(),
                                context.getGlobalAgentId(),
                                agentConfiguration,
                                context.getPersistentStateDirectoryForAgent(agentId()));
        if (isStateStorageRequired() && stateStorage == null) {
            throw new IllegalStateException("State storage is required but not configured");
        }

        String deletedObjectsTopic = getDeletedObjectsTopic();
        if (deletedObjectsTopic != null) {
            deletedObjectsProducer =
                    agentContext
                            .getTopicConnectionProvider()
                            .createProducer(
                                    agentContext.getGlobalAgentId(), deletedObjectsTopic, Map.of());
            deletedObjectsProducer.start();
        }
        String sourceActivitySummaryTopic = getSourceActivitySummaryTopic();
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

    @Override
    public List<Record> read() throws Exception {
        synchronized (this) {
            final String bucketName = getBucketName();
            Objects.requireNonNull(bucketName, "bucketName is required");
            sendSourceActivitySummaryIfNeeded();
            final Collection<StorageProviderObjectReference> objects = listObjects();
            final StorageProviderObjectReference object = getNextObject(objects);
            if (object == null) {
                log.info("No objects to emit");
                checkDeletedObjects(objects, bucketName);
                final int idleTime = getIdleTime();
                log.info("sleeping for {} seconds", idleTime);
                Thread.sleep(idleTime * 1000L);
                return List.of();
            }
            final String name = object.id();
            final long size = object.size();
            if (size < 0) {
                log.info("Found new object {}", name);
            } else {
                log.info("Found new object {}, size {} KB", name, object.size() / 1024);
            }

            try {
                if (isDeleteObjects()) {
                    objectsToCommit.add(name);
                }
                final byte[] read = downloadObject(object);

                final String contentDiff;
                if (stateStorage != null) {
                    T state = getOrInitState();
                    final String key = formatAllTimeObjectsKey(name);
                    String oldValue = state.getAllTimeObjects().put(key, object.contentDigest());
                    StorageProviderSourceState.ObjectDetail e =
                            new StorageProviderSourceState.ObjectDetail(
                                    bucketName, object.id(), System.currentTimeMillis());
                    if (oldValue == null) {
                        contentDiff = "new";
                        state.getCurrentSourceActivitySummary().newObjects().add(e);
                    } else {
                        contentDiff = "content_changed";
                        state.getCurrentSourceActivitySummary().updatedObjects().add(e);
                    }
                    stateStorage.store(state);
                } else {
                    contentDiff = "no_state_storage";
                }
                processed(0, 1);

                List<Header> allHeaders = new ArrayList<>(getSourceRecordHeaders());
                allHeaders.addAll(object.additionalRecordHeaders());
                allHeaders.add(new SimpleRecord.SimpleHeader("name", name));
                allHeaders.add(new SimpleRecord.SimpleHeader("bucket", bucketName));
                allHeaders.add(new SimpleRecord.SimpleHeader("content_diff", contentDiff));
                SimpleRecord record =
                        SimpleRecord.builder().key(name).value(read).headers(allHeaders).build();
                return List.of(record);
            } catch (Exception e) {
                log.error("Error reading object {}", name, e);
                throw e;
            }
        }
    }

    private void sendSourceActivitySummaryIfNeeded() throws Exception {
        if (stateStorage == null) {
            return;
        }
        T state = getOrInitState();
        StorageProviderSourceState.SourceActivitySummary currentSourceActivitySummary =
                state.getCurrentSourceActivitySummary();
        if (currentSourceActivitySummary == null) {
            return;
        }
        List<String> sourceActivitySummaryEvents = getSourceActivitySummaryEvents();
        int countEvents = 0;
        long firstEventTs = Long.MAX_VALUE;
        if (sourceActivitySummaryEvents.contains("new")) {
            countEvents += currentSourceActivitySummary.newObjects().size();
            firstEventTs =
                    currentSourceActivitySummary.newObjects().stream()
                            .mapToLong(StorageProviderSourceState.ObjectDetail::detectedAt)
                            .min()
                            .orElse(Long.MAX_VALUE);
        }
        if (sourceActivitySummaryEvents.contains("updated")) {
            countEvents += currentSourceActivitySummary.updatedObjects().size();
            firstEventTs =
                    Math.min(
                            firstEventTs,
                            currentSourceActivitySummary.updatedObjects().stream()
                                    .mapToLong(StorageProviderSourceState.ObjectDetail::detectedAt)
                                    .min()
                                    .orElse(Long.MAX_VALUE));
        }
        if (sourceActivitySummaryEvents.contains("deleted")) {
            countEvents += currentSourceActivitySummary.deletedObjects().size();
            firstEventTs =
                    Math.min(
                            firstEventTs,
                            currentSourceActivitySummary.deletedObjects().stream()
                                    .mapToLong(StorageProviderSourceState.ObjectDetail::detectedAt)
                                    .min()
                                    .orElse(Long.MAX_VALUE));
        }
        if (countEvents == 0) {
            return;
        }
        long now = System.currentTimeMillis();

        boolean emit = false;
        int sourceActivitySummaryTimeSecondsThreshold =
                getSourceActivitySummaryTimeSecondsThreshold();
        boolean isTimeForStartSummaryOver =
                now >= firstEventTs + sourceActivitySummaryTimeSecondsThreshold * 1000L;
        if (!isTimeForStartSummaryOver) {
            // no time yet, but we have enough events to send
            int sourceActivitySummaryNumThreshold = getSourceActivitySummaryNumEventsThreshold();
            if (sourceActivitySummaryNumThreshold > 0
                    && countEvents >= sourceActivitySummaryNumThreshold) {
                log.info(
                        "Emitting source activity summary, events {} with threshold of {}",
                        countEvents,
                        sourceActivitySummaryNumThreshold);
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
                        "Emitting source activity summary to topic {}",
                        getSourceActivitySummaryTopic());
                // Create a new SourceActivitySummaryWithCounts object directly
                SourceActivitySummaryWithCounts summaryWithCounts =
                        new SourceActivitySummaryWithCounts(
                                currentSourceActivitySummary.newObjects(),
                                currentSourceActivitySummary.updatedObjects(),
                                currentSourceActivitySummary.deletedObjects(),
                                currentSourceActivitySummary.newObjects().size(),
                                currentSourceActivitySummary.updatedObjects().size(),
                                currentSourceActivitySummary.deletedObjects().size());

                // Convert the new object to JSON
                String value = MAPPER.writeValueAsString(summaryWithCounts);
                SimpleRecord simpleRecord = buildSimpleRecord(value, "sourceActivitySummary");
                sourceActivitySummaryProducer.write(simpleRecord).get();
            } else {
                log.warn("No source activity summary producer configured, event will be lost");
            }
            state.setCurrentSourceActivitySummary(
                    new StorageProviderSourceState.SourceActivitySummary(
                            new ArrayList<>(), new ArrayList<>(), new ArrayList<>()));
            stateStorage.store(state);
        }
    }

    private SimpleRecord buildSimpleRecord(String value, String recordType) {
        // Add record type to the headers
        List<Header> allHeaders = new ArrayList<>(getSourceRecordHeaders());
        allHeaders.add(new SimpleRecord.SimpleHeader("recordType", recordType));
        allHeaders.add(new SimpleRecord.SimpleHeader("recordSource", "storageProvider"));

        return SimpleRecord.builder().key(getBucketName()).value(value).headers(allHeaders).build();
    }

    private void checkDeletedObjects(
            Collection<StorageProviderObjectReference> objects, String bucketName)
            throws Exception {
        if (stateStorage != null) {
            log.info("Checking for deleted objects");
            T state = getOrInitState();

            final Set<String> allNames = new HashSet<>();
            for (StorageProviderObjectReference o : objects) {
                allNames.add(formatAllTimeObjectsKey(o.id()));
            }
            Set<String> toRemove = new HashSet<>();
            for (String allTimeKey : state.getAllTimeObjects().keySet()) {
                if (!allNames.contains(allTimeKey)) {
                    log.info("Object {} was deleted", allTimeKey);
                    toRemove.add(allTimeKey);
                }
            }
            for (String allTimeKey : toRemove) {
                state.getAllTimeObjects().remove(allTimeKey);
                String objectName = allTimeKey.replace(bucketName + "@", "");
                state.getCurrentSourceActivitySummary()
                        .deletedObjects()
                        .add(
                                new StorageProviderSourceState.ObjectDetail(
                                        bucketName, objectName, System.currentTimeMillis()));
                if (deletedObjectsProducer != null) {
                    SimpleRecord simpleRecord =
                            buildSimpleRecord(objectName, "sourceObjectDeleted");
                    deletedObjectsProducer.write(simpleRecord).get();
                }
            }
            if (!toRemove.isEmpty()) {
                stateStorage.store(state);
            }
        }
    }

    private StorageProviderObjectReference getNextObject(
            Iterable<StorageProviderObjectReference> results) throws Exception {
        T state = null;
        if (stateStorage != null) {
            log.info("Checking for deleted objects");
            state = getOrInitState();
        }
        for (StorageProviderObjectReference object : results) {
            String name = object.id();
            if (!objectsToCommit.contains(name) || !isDeleteObjects()) {
                if (state != null) {
                    String allTimeDigest =
                            state.getAllTimeObjects().get(formatAllTimeObjectsKey(name));
                    if (allTimeDigest != null && allTimeDigest.equals(object.contentDigest())) {
                        log.info("Object {} didn't change since last time, skipping", name);
                        continue;
                    }
                }
                return object;
            } else {
                log.info("Skipping already processed object {}", name);
            }
        }
        return null;
    }

    private T getOrInitState() throws Exception {
        T state;
        Class<T> stateClass = getStateClass();
        state = stateStorage.get(stateClass);
        if (state == null) {
            state = getStateClass().getConstructor().newInstance();
        }
        return state;
    }

    private String formatAllTimeObjectsKey(String name) {
        return getBucketName() + "@" + name;
    }

    @Override
    protected Map<String, Object> buildAdditionalInfo() {
        return Map.of("bucketName", getBucketName());
    }

    @Override
    public void commit(List<Record> records) throws Exception {
        if (!isDeleteObjects()) {
            return;
        }
        for (Record record : records) {
            String objectName = (String) record.key();
            log.info("Removing object {}", objectName);
            deleteObject(objectName);
            objectsToCommit.remove(objectName);
        }
    }

    @Override
    public void onSignal(Record record) throws Exception {
        Object key = record.key();
        Object value = record.value();
        if (key == null) {
            log.warn("skipping signal with null key {}", record);
            return;
        }
        if (stateStorage == null) {
            log.warn("skipping signal, no state storage configured");
            return;
        }
        synchronized (this) {
            T state = getOrInitState();
            switch (key.toString()) {
                case "invalidate":
                    if (value instanceof String objectName) {
                        log.info("Invalidating object {}", objectName);
                        state.getAllTimeObjects().remove(formatAllTimeObjectsKey(objectName));
                    } else {
                        log.warn(
                                "Invalid signal value type, expected String, got {}",
                                value.getClass());
                        return;
                    }
                    break;
                case "invalidate-all":
                    log.info("Invaliding all objects, size {}", state.getAllTimeObjects().size());
                    state.getAllTimeObjects().clear();
                    break;
                default:
                    log.warn("Unknown signal key {}", key);
                    return;
            }
            stateStorage.store(state);
        }
    }

    @Override
    public void close() {
        super.close();
        if (deletedObjectsProducer != null) {
            deletedObjectsProducer.close();
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
        StateStorage<T> tStateStorage =
                new StateStorageProvider<T>()
                        .create(
                                context.getTenant(),
                                agentId(),
                                context.getGlobalAgentId(),
                                configuration,
                                context.getPersistentStateDirectoryForAgent(agentId()));
        if (tStateStorage != null) {
            tStateStorage.delete();
        }
    }
}
