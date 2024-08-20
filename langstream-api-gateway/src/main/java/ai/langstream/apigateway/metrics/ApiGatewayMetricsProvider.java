package ai.langstream.apigateway.metrics;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApiGatewayMetricsProvider {

    @Bean(destroyMethod = "close")
    public ApiGatewayMetrics apiGatewayMetrics() {
        return new ApiGatewayMetrics();
    }
}
