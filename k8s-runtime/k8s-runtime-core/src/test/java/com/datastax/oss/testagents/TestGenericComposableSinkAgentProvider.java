package com.datastax.oss.testagents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Pipeline;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.Connection;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.Topic;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;
import com.datastax.oss.sga.runtime.impl.k8s.KubernetesClusterRuntime;
import com.datastax.oss.sga.runtime.impl.k8s.agents.AbstractComposableAgentProvider;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestGenericComposableSinkAgentProvider extends AbstractComposableAgentProvider {

    public TestGenericComposableSinkAgentProvider() {
        super(Set.of("generic-composable-sink"), List.of(KubernetesClusterRuntime.CLUSTER_TYPE, "none"));
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.SINK;
    }

}
