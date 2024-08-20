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
package ai.langstream.deployer.k8s.api.crds.apps;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ApplicationSpecOptions {

    public static String RUNTIME_VERSION_NO_UPGRADE = "no-upgrade";
    public static String RUNTIME_VERSION_AUTO_UPGRADE = "auto-upgrade";

    public enum DeleteMode {
        CLEANUP_REQUIRED,
        CLEANUP_BEST_EFFORT;
    }

    private DeleteMode deleteMode = DeleteMode.CLEANUP_REQUIRED;
    private boolean markedForDeletion;
    private long seed;
    private String runtimeVersion;
    private boolean autoUpgradeRuntimeImagePullPolicy;
    private boolean autoUpgradeAgentResources;
    private boolean autoUpgradeAgentPodTemplate;
}
