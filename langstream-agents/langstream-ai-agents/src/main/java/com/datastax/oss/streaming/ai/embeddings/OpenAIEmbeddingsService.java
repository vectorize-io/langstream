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
package com.datastax.oss.streaming.ai.embeddings;

import ai.langstream.api.runner.code.MetricsReporter;
import com.azure.ai.openai.OpenAIAsyncClient;
import com.azure.ai.openai.models.EmbeddingItem;
import com.azure.ai.openai.models.EmbeddingsOptions;
import com.azure.ai.openai.models.EmbeddingsUsage;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OpenAIEmbeddingsService implements EmbeddingsService {

    private final OpenAIAsyncClient openAIClient;
    private final String model;
    private final Integer dimensions;

    private final MetricsReporter.Counter totalTokens;
    private final MetricsReporter.Counter promptTokens;
    private final MetricsReporter.Counter numCalls;
    private final MetricsReporter.Counter numTexts;
    private final MetricsReporter.Counter numErrors;

    public OpenAIEmbeddingsService(
            OpenAIAsyncClient openAIClient,
            String model,
            MetricsReporter metricsReporter,
            Integer dimensions) {
        this.openAIClient = openAIClient;
        this.model = model;
        this.dimensions = dimensions;
        this.totalTokens =
                metricsReporter.counter(
                        "openai_embeddings_total_tokens",
                        "Total number of tokens exchanged with OpenAI");
        this.promptTokens =
                metricsReporter.counter(
                        "openai_embeddings_prompt_tokens",
                        "Total number of prompt tokens sent to OpenAI");
        this.numCalls =
                metricsReporter.counter(
                        "openai_embeddings_num_calls", "Total number of calls to OpenAI");
        this.numTexts =
                metricsReporter.counter(
                        "openai_embeddings_num_texts", "Total number of texts sent to OpenAI");
        this.numErrors =
                metricsReporter.counter(
                        "openai_embeddings_num_errors",
                        "Total number of errors while calling OpenAI");
    }

    @Override
    public CompletableFuture<List<List<Double>>> computeEmbeddings(List<String> texts) {
        try {
            EmbeddingsOptions embeddingsOptions = new EmbeddingsOptions(texts);
            if (dimensions > 0) {
                log.debug("Setting embedding dimensions to {}", dimensions);
                embeddingsOptions.setDimensions(dimensions);
            }
            numCalls.count(1);
            numTexts.count(texts.size());

            CompletableFuture<List<List<Double>>> result =
                    openAIClient
                            .getEmbeddings(model, embeddingsOptions)
                            .toFuture()
                            .thenApply(
                                    embeddings -> {
                                        EmbeddingsUsage usage = embeddings.getUsage();
                                        totalTokens.count(usage.getTotalTokens());
                                        promptTokens.count(usage.getPromptTokens());
                                        return embeddings.getData().stream()
                                                .map(EmbeddingItem::getEmbedding)
                                                .map(this::convertToDoubleList)
                                                .collect(Collectors.toList());
                                    });

            result.exceptionally(
                    err -> {
                        numErrors.count(1);
                        return null;
                    });

            return result;
        } catch (RuntimeException err) {
            log.error("Cannot compute embeddings", err);
            return CompletableFuture.failedFuture(err);
        }
    }

    private List<Double> convertToDoubleList(List<Float> floatList) {
        // Log the length of the float list
        log.debug("Float list length: {}", floatList.size());
        return floatList.stream().map(Float::doubleValue).collect(Collectors.toList());
    }
}
