package ai.langstream.ai.agents.services.impl;

import com.azure.ai.openai.OpenAIAsyncClient;
import com.azure.ai.openai.OpenAIClient;
import com.azure.ai.openai.OpenAIClientBuilder;
import com.azure.ai.openai.models.ChatCompletionsOptions;
import com.azure.ai.openai.models.ChatRequestMessage;
import com.azure.ai.openai.models.ChatRequestUserMessage;
import com.azure.ai.openai.models.EmbeddingItem;
import com.azure.ai.openai.models.Embeddings;
import com.azure.ai.openai.models.EmbeddingsOptions;
import com.azure.ai.openai.models.EmbeddingsUsage;
import com.azure.core.credential.AzureKeyCredential;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class OpenAITest {

    private OpenAIAsyncClient openAIClient;

    @BeforeEach
    void setup() {
        openAIClient =
                new OpenAIClientBuilder()
                        .credential(new AzureKeyCredential("YOUR_OPENAI_KEY"))
                        .buildAsyncClient();
    }

    @Disabled
    @Test
    void testRealChatCompletions() throws Exception {
        List<ChatRequestMessage> chatMessages = new ArrayList<>();
        chatMessages.add(new ChatRequestUserMessage("Name the US presidents of the 20th century"));

        ChatCompletionsOptions options = new ChatCompletionsOptions(chatMessages);

        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        openAIClient
                .getChatCompletionsStream("gpt-3.5-turbo", options)
                .doOnNext(
                        chatCompletions -> {
                            String response =
                                    chatCompletions.getChoices().get(0).getDelta().getContent();
                            System.out.println("Response: " + response);
                            if (response == null) {
                                completableFuture.complete(null);
                            }
                        })
                .doOnError(completableFuture::completeExceptionally)
                .subscribe();

        completableFuture.join();
    }

    @Disabled
    @Test
    void testRealEmbeddings() throws Exception {
        String azureOpenaiKey = "";
        String deploymentOrModelId = "text-embedding-ada-002";

        OpenAIClient client =
                new OpenAIClientBuilder()
                        .credential(new AzureKeyCredential(azureOpenaiKey))
                        .buildClient();

        EmbeddingsOptions embeddingsOptions =
                new EmbeddingsOptions(Arrays.asList("Your text string goes here"));

        Embeddings embeddings = client.getEmbeddings(deploymentOrModelId, embeddingsOptions);

        for (EmbeddingItem item : embeddings.getData()) {
            System.out.printf("Index: %d.%n", item.getPromptIndex());
            System.out.println(
                    "Embedding as base64 encoded string: " + item.getEmbeddingAsString());
            System.out.println("Embedding as list of floats: ");
            for (Float embedding : item.getEmbedding()) {
                System.out.printf("%f;", embedding);
            }
        }

        EmbeddingsUsage usage = embeddings.getUsage();
        System.out.printf(
                "Usage: number of prompt token is %d and number of total tokens in request and response is %d.%n",
                usage.getPromptTokens(), usage.getTotalTokens());
    }
}
