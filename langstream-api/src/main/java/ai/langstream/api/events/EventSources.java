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
package ai.langstream.api.events;

import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.model.Gateway;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class EventSources {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ApplicationSource {
        private String tenant;
        private String applicationId;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GatewaySource extends ApplicationSource {
        private Gateway gateway;

        @Builder
        public GatewaySource(String tenant, String applicationId, Gateway gateway) {
            super(tenant, applicationId);
            this.gateway = gateway;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AssetSource extends ApplicationSource {
        private AssetDefinition asset;

        @Builder
        public AssetSource(String tenant, String applicationId, AssetDefinition asset) {
            super(tenant, applicationId);
            this.asset = asset;
        }
    }
}
