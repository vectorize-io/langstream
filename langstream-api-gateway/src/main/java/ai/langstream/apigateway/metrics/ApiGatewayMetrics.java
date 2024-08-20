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

import static ai.langstream.apigateway.metrics.MetricsNames.METRIC_GATEWAYS_HTTP_REQUESTS;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ApiGatewayMetrics implements AutoCloseable {

    private static final String TAG_TENANT = "tenant";
    private static final String TAG_APPLICATION_ID = "application";
    private static final String TAG_GATEWAY_ID = "gateway";

    private final MeterRegistry meterRegistry;

    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    public Timer.Sample startTimer() {
        return Timer.start(getMeterRegistry());
    }

    public void recordHttpGatewayRequest(
            io.micrometer.core.instrument.Timer.Sample sample,
            String tenant,
            String applicationId,
            String gatewayId,
            int responseStatusCode) {
        Timer timer =
                Timer.builder(METRIC_GATEWAYS_HTTP_REQUESTS)
                        .description("HTTP requests to gateways")
                        .tag(TAG_TENANT, tenant)
                        .tag(TAG_APPLICATION_ID, applicationId)
                        .tag(TAG_GATEWAY_ID, gatewayId)
                        .tag("response_status_code", responseStatusCode + "")
                        .publishPercentiles(0.5, 0.95, 0.99)
                        .register(getMeterRegistry());
        sample.stop(timer);
    }

    @Override
    public void close() throws Exception {
        meterRegistry.clear();
    }
}
