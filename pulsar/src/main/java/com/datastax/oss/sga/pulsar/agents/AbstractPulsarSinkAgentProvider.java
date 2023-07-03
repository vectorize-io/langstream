package com.datastax.oss.sga.pulsar.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.runtime.ClusterRuntime;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;
import com.datastax.oss.sga.pulsar.PulsarName;
import com.datastax.oss.sga.pulsar.PulsarPhysicalApplicationInstance;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

public abstract class AbstractPulsarSinkAgentProvider extends AbstractPulsarAgentProvider {

    public AbstractPulsarSinkAgentProvider(List<String> supportedTypes, List<String> supportedClusterTypes) {
        super(supportedTypes, supportedClusterTypes);
    }

    protected abstract String getSinkType(AgentConfiguration agentConfiguration);

    @AllArgsConstructor
    @Data
    public static class PulsarSinkMetadata {
        private final PulsarName pulsarName;
        private final String sinkType;
    }

    @Override
    protected Object computeAgentMetadata(AgentConfiguration agentConfiguration, PhysicalApplicationInstance physicalApplicationInstance, ClusterRuntime clusterRuntime) {
        PulsarPhysicalApplicationInstance pulsar = (PulsarPhysicalApplicationInstance) physicalApplicationInstance;
        PulsarName pulsarName = computePulsarName(pulsar, agentConfiguration);
        return new PulsarSinkMetadata(pulsarName, getSinkType(agentConfiguration));
    }


}
