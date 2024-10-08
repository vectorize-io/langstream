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
package ai.langstream.apigateway.metrics;

public class MetricsNames {
    // guava cache metrics are exposed as label value
    public static final String GUAVA_CACHE_TOPIC_PRODUCER = "langstream_topic_producer_cache";
    public static final String GUAVA_CACHE_TOPIC_CONNECTIONS_RUNTIME_CACHE =
            "langstream_topic_connections_runtime_cache";

    // metric names are converted by micrometer to naming conventions
    public static final String METRIC_GATEWAYS_HTTP_REQUESTS = "langstream.gateways.http.requests";
}
