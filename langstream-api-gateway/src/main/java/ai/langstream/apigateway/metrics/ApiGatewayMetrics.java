package ai.langstream.apigateway.metrics;

import ai.langstream.api.model.Gateway;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;

public class ApiGatewayMetrics implements AutoCloseable {

    private final static String TAG_TENANT = "tenant";
    private final static String TAG_APPLICATION_ID = "application";
    private final static String TAG_GATEWAY_ID = "gateway";

    public void addHttpGatewayRequest(
            String tenant,
            String applicationId,
            String gatewayId,
            String httpMethod,
            int responseStatusCode) {
        Counter.builder("langstream.gateways.http.requests")
                .description("HTTP requests to gateways")
                .tag(TAG_TENANT, tenant)
                .tag(TAG_APPLICATION_ID, applicationId)
                .tag(TAG_GATEWAY_ID, gatewayId)
                .tag("http_method", httpMethod)
                .tag("response_status_code", responseStatusCode + "")
                .register(Metrics.globalRegistry)
                .increment();
    }

    @Override
    public void close() throws Exception {
        Metrics.globalRegistry.close();
        Metrics.globalRegistry.clear();
    }
}