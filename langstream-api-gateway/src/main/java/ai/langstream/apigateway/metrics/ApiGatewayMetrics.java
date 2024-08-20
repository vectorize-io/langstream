package ai.langstream.apigateway.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

public class ApiGatewayMetrics implements AutoCloseable {

    private static final String TAG_TENANT = "tenant";
    private static final String TAG_APPLICATION_ID = "application";
    private static final String TAG_GATEWAY_ID = "gateway";

    public MeterRegistry getMeterRegistry() {
        return Metrics.globalRegistry;
    }

    public Timer.Sample startTimer() {
        return Timer.start(getMeterRegistry());
    }

    public void recordHttpGatewayRequest(
            io.micrometer.core.instrument.Timer.Sample sample,
            String tenant,
            String applicationId,
            String gatewayId,
            String httpMethod,
            int responseStatusCode) {
        Timer timer =
                Timer.builder("langstream.gateways.http.requests")
                        .description("HTTP requests to gateways")
                        .tag(TAG_TENANT, tenant)
                        .tag(TAG_APPLICATION_ID, applicationId)
                        .tag(TAG_GATEWAY_ID, gatewayId)
                        .tag("http_method", httpMethod)
                        .tag("response_status_code", responseStatusCode + "")
                        .publishPercentiles(0.5, 0.95, 0.99)
                        .register(getMeterRegistry());
        sample.stop(timer);
    }

    @Override
    public void close() throws Exception {
        Metrics.globalRegistry.close();
        Metrics.globalRegistry.clear();
    }
}
