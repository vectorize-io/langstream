package ai.langstream.apigateway.metrics;

import ai.langstream.api.model.Gateway;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.apigateway.config.TopicProperties;
import ai.langstream.apigateway.gateways.LRUTopicProducerCache;
import ai.langstream.apigateway.gateways.TopicProducerCache;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.cache.GuavaCacheMetrics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Supplier;

@Configuration
public class ApiGatewayMetricsProvider {


    @Bean(destroyMethod = "close")
    public ApiGatewayMetrics apiGatewayMetrics() {
        return new ApiGatewayMetrics();
    }

}
