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
