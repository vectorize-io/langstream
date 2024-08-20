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
package ai.langstream.apigateway;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;

public class ApiGatewayTestUtil {

    public static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    @SneakyThrows
    public static String getPrometheusMetrics(final int port) {
        final String url = "http://localhost:%d/management/prometheus".formatted(port);
        final HttpRequest request = HttpRequest.newBuilder(URI.create(url)).GET().build();
        final HttpResponse<String> response =
                HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        return response.body();
    }

    public record ParsedMetric(String name, String value, Map<String, String> labels) {}

    public static List<ParsedMetric> findMetric(String metricName, String metrics) {
        return metrics.lines()
                .filter(
                        m -> {
                            if (m.startsWith("#")) {
                                return false;
                            }
                            return m.startsWith(metricName + "{");
                        })
                .map(
                        line -> {
                            final String[] parts = line.split(" ");
                            String name = parts[0];
                            final String value = parts[1];
                            final String labels =
                                    name.substring(name.indexOf("{") + 1, name.indexOf("}"));
                            name = name.substring(0, name.indexOf("{"));
                            final Map<String, String> labelMap = new HashMap<>();
                            for (String label : labels.split(",")) {
                                final String[] labelParts = label.split("=");
                                labelMap.put(
                                        labelParts[0],
                                        labelParts[1].substring(1, labelParts[1].length() - 1));
                            }
                            return new ParsedMetric(name, value, labelMap);
                        })
                .collect(Collectors.toList());
    }
}
