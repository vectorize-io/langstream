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
package ai.langstream.deployer.k8s;

import io.fabric8.kubernetes.api.model.NodeAffinity;
import io.fabric8.kubernetes.api.model.Toleration;
import java.util.List;
import java.util.Map;
import lombok.Getter;

public record PodTemplate(
        List<Toleration> tolerations,
        Map<String, String> nodeSelector,
        Map<String, String> annotations,
        NodeAffinity nodeAffinity,
        PodAntiAffinityConfig podAntiAffinity) {

    public record PodAntiAffinityConfig(TopologyKey topologyKey, boolean required) {
        public enum TopologyKey {
            HOST("kubernetes.io/hostname"),
            ZONE("topology.kubernetes.io/zone"),
            REGION("topology.kubernetes.io/region");

            @Getter private final String key;

            TopologyKey(String key) {
                this.key = key;
            }
        }
    }

    public static PodTemplate merge(PodTemplate primary, PodTemplate secondary) {
        return new PodTemplate(
                merge(primary.tolerations(), secondary.tolerations()),
                merge(primary.nodeSelector(), secondary.nodeSelector()),
                merge(primary.annotations(), secondary.annotations()),
                primary.nodeAffinity() != null ? primary.nodeAffinity() : secondary.nodeAffinity(),
                primary.podAntiAffinity() != null
                        ? primary.podAntiAffinity()
                        : secondary.podAntiAffinity());
    }

    private static Map<String, String> merge(
            Map<String, String> primary, Map<String, String> secondary) {
        if (primary == null || primary.isEmpty()) {
            return secondary;
        } else {
            return primary;
        }
    }

    private static <T> List<T> merge(List<T> primary, List<T> secondary) {
        if (primary == null || primary.isEmpty()) {
            return secondary;
        } else {
            return primary;
        }
    }
}
