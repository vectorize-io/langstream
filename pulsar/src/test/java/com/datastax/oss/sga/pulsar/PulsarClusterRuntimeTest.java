package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.Connection;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.AgentImplementation;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import com.datastax.oss.sga.pulsar.agents.AbstractPulsarSinkAgentProvider;
import com.datastax.oss.sga.pulsar.agents.AbstractPulsarSourceAgentProvider;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class PulsarClusterRuntimeTest {

    @Test
    public void testMapCassandraSink() throws Exception {
        ApplicationInstance applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        """
                                instance:
                                  streamingCluster:
                                    type: "pulsar"
                                    configuration:                                      
                                      webServiceUrl: "http://localhost:8080"
                                      defaultTenant: "public"
                                      defaultNamespace: "default"
                                """,
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                topics:
                                  - name: "input-topic-cassandra"
                                    creation-mode: create-if-not-exists
                                    schema:
                                      type: avro
                                      schema: '{"type":"record","namespace":"examples","name":"Product","fields":[{"name":"id","type":"string"},{"name":"name","type":"string"},{"name":"description","type":"string"},{"name":"price","type":"double"},{"name":"category","type":"string"},{"name":"item_vector","type":"bytes"}]}}'
                                pipeline:
                                  - name: "sink1"
                                    id: "sink-1-id"
                                    type: "cassandra-sink"
                                    input: "input-topic-cassandra"
                                    configuration:
                                      mappings: "id=value.id,name=value.name,description=value.description,item_vector=value.item_vector"
                                """));

        ApplicationDeployer<PulsarPhysicalApplicationInstance> deployer = ApplicationDeployer
                .<PulsarPhysicalApplicationInstance>builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        PulsarPhysicalApplicationInstance implementation = deployer.createImplementation(applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module, new Connection(new TopicDefinition("input-topic-cassandra", null, null))) instanceof PulsarTopic);
        PulsarName pulsarName = new PulsarName("public", "default", "input-topic-cassandra");
        assertTrue(implementation.getTopics().containsKey(pulsarName));

        AgentImplementation agentImplementation = implementation.getAgentImplementation(module, "sink-1-id");
        assertNotNull(agentImplementation);
        AbstractAgentProvider.DefaultAgentImplementation genericSink = (AbstractAgentProvider.DefaultAgentImplementation) agentImplementation;
        AbstractPulsarSinkAgentProvider.PulsarSinkMetadata pulsarSinkMetadata = genericSink.getPhysicalMetadata();
        assertEquals("cassandra-enhanced", pulsarSinkMetadata.getSinkType());
        assertEquals(new PulsarName("public", "default", "sink1"), pulsarSinkMetadata.getPulsarName());

    }

    @Test
    public void testMapGenericPulsarSink() throws Exception {
        ApplicationInstance applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        """
                                instance:
                                  streamingCluster:
                                    type: "pulsar"
                                    configuration:                                      
                                      webServiceUrl: "http://localhost:8080"
                                      defaultTenant: "public"
                                      defaultNamespace: "default"
                                """,
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                    schema:
                                      type: avro
                                      schema: '{"type":"record","namespace":"examples","name":"Product","fields":[{"name":"id","type":"string"},{"name":"name","type":"string"},{"name":"description","type":"string"},{"name":"price","type":"double"},{"name":"category","type":"string"},{"name":"item_vector","type":"bytes"}]}}'
                                pipeline:
                                  - name: "sink1"
                                    id: "sink-1-id"
                                    type: "generic-pulsar-sink"
                                    input: "input-topic"
                                    configuration:
                                      sinkType: "some-sink-type-on-your-cluster"
                                      config1: "value"
                                      config2: "value2"
                                """));

        ApplicationDeployer<PulsarPhysicalApplicationInstance> deployer = ApplicationDeployer
                .<PulsarPhysicalApplicationInstance>builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        PulsarPhysicalApplicationInstance implementation = deployer.createImplementation(applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module, new Connection(new TopicDefinition("input-topic", null, null))) instanceof PulsarTopic);
        PulsarName pulsarName = new PulsarName("public", "default", "input-topic");
        assertTrue(implementation.getTopics().containsKey(pulsarName));

        AgentImplementation agentImplementation = implementation.getAgentImplementation(module, "sink-1-id");
        assertNotNull(agentImplementation);
        AbstractAgentProvider.DefaultAgentImplementation genericSink = (AbstractAgentProvider.DefaultAgentImplementation) agentImplementation;
        AbstractPulsarSinkAgentProvider.PulsarSinkMetadata pulsarSinkMetadata = genericSink.getPhysicalMetadata();
        assertEquals("some-sink-type-on-your-cluster", pulsarSinkMetadata.getSinkType());
        assertEquals(new PulsarName("public", "default", "sink1"), pulsarSinkMetadata.getPulsarName());

    }

    @Test
    public void testMapGenericPulsarSource() throws Exception {
        ApplicationInstance applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        """
                                instance:
                                  streamingCluster:
                                    type: "pulsar"
                                    configuration:                                      
                                      webServiceUrl: "http://localhost:8080"
                                      defaultTenant: "public"
                                      defaultNamespace: "default"
                                """,
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                topics:
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "source1"
                                    id: "source-1-id"
                                    type: "generic-pulsar-source"
                                    output: "output-topic"
                                    configuration:
                                      sourceType: "some-source-type-on-your-cluster"
                                      config1: "value"
                                      config2: "value2"
                                """));

        ApplicationDeployer<PulsarPhysicalApplicationInstance> deployer = ApplicationDeployer
                .<PulsarPhysicalApplicationInstance>builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        PulsarPhysicalApplicationInstance implementation = deployer.createImplementation(applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module, new Connection(new TopicDefinition("output-topic", null, null))) instanceof PulsarTopic);
        PulsarName pulsarName = new PulsarName("public", "default", "output-topic");
        assertTrue(implementation.getTopics().containsKey(pulsarName));

        AgentImplementation agentImplementation = implementation.getAgentImplementation(module, "source-1-id");
        assertNotNull(agentImplementation);
        AbstractAgentProvider.DefaultAgentImplementation genericSink = (AbstractAgentProvider.DefaultAgentImplementation) agentImplementation;
        AbstractPulsarSourceAgentProvider.PulsarSourceMetadata pulsarSourceMetadata = genericSink.getPhysicalMetadata();
        assertEquals("some-source-type-on-your-cluster", pulsarSourceMetadata.getSourceType());
        assertEquals(new PulsarName("public", "default", "source1"), pulsarSourceMetadata.getPulsarName());

    }


    @Test
    @Disabled
    public void testOpenAIComputeEmbeddingFunction() throws Exception {
        ApplicationInstance applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        """
                                instance:
                                  streamingCluster:
                                    type: "pulsar"
                                    configuration:                                      
                                      webServiceUrl: "http://localhost:8080"
                                      defaultTenant: "public"
                                      defaultNamespace: "default"
                                """,
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                    schema:
                                      type: avro
                                      schema: '{"type":"record","namespace":"examples","name":"Product","fields":[{"name":"id","type":"string"},{"name":"name","type":"string"},{"name":"description","type":"string"},{"name":"price","type":"double"},{"name":"category","type":"string"},{"name":"item_vector","type":"bytes"}]}}'
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists                                    
                                pipeline:
                                  - name: "compute-embeddings"
                                    id: "step1"
                                    type: "compute-ai-embeddings"
                                    input: "input-topic"
                                    output: "output-topic"
                                    configuration:                                      
                                      model: "text-embedding-ada-002"
                                      embeddings-field: "value.embeddings"
                                      text: "{{ value.name }} {{ value.description }}"
                                """));

        ApplicationDeployer<PulsarPhysicalApplicationInstance> deployer = ApplicationDeployer
                .<PulsarPhysicalApplicationInstance>builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        PulsarPhysicalApplicationInstance implementation = deployer.createImplementation(applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module, new Connection(new TopicDefinition("input-topic", null, null))) instanceof PulsarTopic);
        assertTrue(implementation.getConnectionImplementation(module, new Connection(new TopicDefinition("output-topic", null, null))) instanceof PulsarTopic);

        AgentImplementation agentImplementation = implementation.getAgentImplementation(module, "step1");
        assertNotNull(agentImplementation);

    }

}