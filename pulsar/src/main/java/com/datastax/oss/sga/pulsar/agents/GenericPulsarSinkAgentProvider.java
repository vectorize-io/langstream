package com.datastax.oss.sga.pulsar.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.pulsar.PulsarClusterRuntime;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class GenericPulsarSinkAgentProvider extends AbstractPulsarAgentProvider {

    public GenericPulsarSinkAgentProvider() {
        super(Set.of("sink"), List.of(PulsarClusterRuntime.CLUSTER_TYPE));
    }

    @Override
    protected String getAgentType(AgentConfiguration configuration) {
        String sinkType = (String)configuration.getConfiguration()
                .get("sinkType");
        if (sinkType == null) {
            throw new IllegalArgumentException("For the generic pulsar-sink you must configured the sinkType configuration property");
        }
        return sinkType;
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.SINK;
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module, ExecutionPlan physicalApplicationInstance, ComputeClusterRuntime clusterRuntime) {
        Map<String, Object> copy = super.computeAgentConfiguration(agentConfiguration, module, physicalApplicationInstance, clusterRuntime);
        copy.remove("sinkType");
        return copy;
    }
}
