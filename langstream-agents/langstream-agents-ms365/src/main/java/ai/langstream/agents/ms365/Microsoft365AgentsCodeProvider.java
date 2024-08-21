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
package ai.langstream.agents.ms365;

import ai.langstream.agents.ms365.sharepoint.SharepointSource;
import ai.langstream.api.runner.code.AgentCode;
import ai.langstream.api.runner.code.AgentCodeProvider;

import java.util.List;

public class Microsoft365AgentsCodeProvider implements AgentCodeProvider {

    public static final String SHAREPOINT_SOURCE = "ms365-sharepoint-source";
    private static final List<String> AGENTS =
            List.of(SHAREPOINT_SOURCE);

    @Override
    public boolean supports(String agentType) {
        return AGENTS.contains(agentType);
    }

    @Override
    public AgentCode createInstance(String agentType) {
        switch (agentType) {
            case SHAREPOINT_SOURCE:
                return new SharepointSource();
            default:
                throw new IllegalArgumentException("Unsupported agent type: " + agentType);
        }
    }
}
