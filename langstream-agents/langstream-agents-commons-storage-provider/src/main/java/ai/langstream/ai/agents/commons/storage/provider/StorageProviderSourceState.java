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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class StorageProviderSourceState {

    public record ObjectDetail(String bucket, String object, long detectedAt) {}

    public record SourceActivitySummary(
            List<ObjectDetail> newObjects,
            List<ObjectDetail> updatedObjects,
            List<ObjectDetail> deletedObjects) {}

    private Map<String, String> allTimeObjects = new HashMap<>();
    private SourceActivitySummary currentSourceActivitySummary =
            new SourceActivitySummary(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
}
