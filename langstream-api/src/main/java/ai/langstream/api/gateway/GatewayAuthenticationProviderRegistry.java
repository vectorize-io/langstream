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
package ai.langstream.api.gateway;

import ai.langstream.api.util.ObjectMapperFactory;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import lombok.SneakyThrows;

public class GatewayAuthenticationProviderRegistry {
    record ProviderCacheKey(String type, String configString) {}

    private static final Map<ProviderCacheKey, GatewayAuthenticationProvider> cachedProviders =
            new ConcurrentHashMap<>();

    @SneakyThrows
    public static GatewayAuthenticationProvider loadProvider(
            String type, Map<String, Object> configuration) {
        Objects.requireNonNull(type, "type cannot be null");
        final Map<String, Object> finalConfiguration =
                configuration == null ? Map.of() : configuration;
        ProviderCacheKey key =
                new ProviderCacheKey(
                        type,
                        ObjectMapperFactory.getDefaultMapper()
                                .writeValueAsString(finalConfiguration));
        return cachedProviders.computeIfAbsent(
                key,
                k -> {
                    ServiceLoader<GatewayAuthenticationProvider> loader =
                            ServiceLoader.load(GatewayAuthenticationProvider.class);
                    final GatewayAuthenticationProvider store =
                            loader.stream()
                                    .filter(p -> type.equals(p.get().type()))
                                    .findFirst()
                                    .orElseThrow(
                                            () ->
                                                    new RuntimeException(
                                                            "No GatewayAuthenticationProvider found for type "
                                                                    + type))
                                    .get();
                    store.initialize(finalConfiguration);
                    return store;
                });
    }
}
