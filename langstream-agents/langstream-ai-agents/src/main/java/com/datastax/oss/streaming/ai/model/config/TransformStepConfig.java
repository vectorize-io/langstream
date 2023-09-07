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
package com.datastax.oss.streaming.ai.model.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import lombok.Getter;

@Getter
public class TransformStepConfig {
    @JsonProperty(required = true)
    private List<StepConfig> steps;

    @JsonProperty private OpenAIConfig openai;

    @JsonProperty private HuggingFaceConfig huggingface;

    @JsonProperty private Map<String, Object> datasource;

    @JsonProperty private boolean attemptJsonConversion = true;
}
