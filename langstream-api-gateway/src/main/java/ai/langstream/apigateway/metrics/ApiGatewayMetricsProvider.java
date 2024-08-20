package ai.langstream.apigateway.metrics;

import io.micrometer.core.instrument.Metrics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApiGatewayMetricsProvider {

    @Bean(destroyMethod = "close")
    public ApiGatewayMetrics apiGatewayMetrics() {
        System.out.println("CALL ApiGatewayMetricsProvider");
        return new ApiGatewayMetrics(Metrics.globalRegistry);
    }
}
