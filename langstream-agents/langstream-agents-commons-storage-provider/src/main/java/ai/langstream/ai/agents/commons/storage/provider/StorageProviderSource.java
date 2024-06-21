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
import ai.langstream.api.runner.code.*;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.TopicProducer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class StorageProviderSource<T extends StorageProviderSourceState>
        extends AbstractAgentCode implements AgentSource {

    private Map<String, Object> agentConfiguration;
    private final Set<String> objectsToCommit = ConcurrentHashMap.newKeySet();
    @Getter private StateStorage<T> stateStorage;

    private TopicProducer deletedObjectsProducer;

    public abstract Class<T> getStateClass();

    public abstract void initializeClientAndBucket(Map<String, Object> configuration);

    public abstract String getBucketName();

    public abstract boolean isDeleteObjects();

    public abstract int getIdleTime();

    public abstract String getDeletedObjectsTopic();

    public abstract List<StorageProviderObjectReference> listObjects() throws Exception;

    public abstract byte[] downloadObject(String name) throws Exception;

    public abstract void deleteObject(String name) throws Exception;

    @Override
    public void init(Map<String, Object> configuration) {
        agentConfiguration = configuration;
        initializeClientAndBucket(configuration);
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

        String deletedObjectsTopic = getDeletedObjectsTopic();
        if (deletedObjectsTopic != null) {
            deletedObjectsProducer =
                    agentContext
                            .getTopicConnectionProvider()
                            .createProducer(
                                    agentContext.getGlobalAgentId(), deletedObjectsTopic, Map.of());
            deletedObjectsProducer.start();
        }
    }

    @Override
    public List<Record> read() throws Exception {
        final String bucketName = getBucketName();
        final List<StorageProviderObjectReference> objects = listObjects();
        final StorageProviderObjectReference object = getNextObject(objects);
        if (object == null) {
            log.info("No objects to emit");
            if (stateStorage != null) {
                log.info("Checking for deleted objects");
                T state = getOrInitState();

                final Set<String> allNames = new HashSet<>();
                for (StorageProviderObjectReference o : objects) {
                    allNames.add(formatAllTimeObjectsKey(o.name()));
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
                    if (deletedObjectsProducer != null) {
                        SimpleRecord simpleRecord =
                                SimpleRecord.of(
                                        bucketName, allTimeKey.replace(bucketName + "@", ""));
                        deletedObjectsProducer.write(simpleRecord).get();
                    }
                }
                if (!toRemove.isEmpty()) {
                    stateStorage.store(state);
                }
            }
            final int idleTime = getIdleTime();
            log.info("sleeping for {} seconds", idleTime);
            Thread.sleep(idleTime * 1000L);
            return List.of();
        }
        final String name = object.name();
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
            final byte[] read = downloadObject(object.name());

            final String contentDiff;
            if (stateStorage != null) {
                T state = getOrInitState();
                final String key = formatAllTimeObjectsKey(name);
                String oldValue = state.getAllTimeObjects().put(key, object.contentDigest());
                contentDiff = oldValue == null ? "new" : "content_changed";
                stateStorage.store(state);
            } else {
                contentDiff = "no_state_storage";
            }
            processed(0, 1);
            SimpleRecord record =
                    SimpleRecord.builder()
                            .key(name)
                            .value(read)
                            .headers(
                                    List.of(
                                            new SimpleRecord.SimpleHeader("name", name),
                                            new SimpleRecord.SimpleHeader("bucket", bucketName),
                                            new SimpleRecord.SimpleHeader(
                                                    "content_diff", contentDiff)))
                            .build();
            return List.of(record);
        } catch (Exception e) {
            log.error("Error reading object {}", name, e);
            throw e;
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
            String name = object.name();
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
}
