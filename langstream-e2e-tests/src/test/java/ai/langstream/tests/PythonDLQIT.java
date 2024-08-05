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
package ai.langstream.tests;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import ai.langstream.tests.util.BaseEndToEndTest;
import ai.langstream.tests.util.TestSuites;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(BaseEndToEndTest.class)
@Tag(TestSuites.CATEGORY_OTHER)
public class PythonDLQIT extends BaseEndToEndTest {

    @Test
    public void test() throws Exception {
        assumeTrue(streamingCluster.type().equals("pulsar"));
        installLangStreamCluster(true);
        final String tenant = "ten-" + System.currentTimeMillis();
        setupTenant(tenant);
        final String applicationId = "my-test-app";
        Map<String, String> appEnv = new HashMap<>();
        Map<String, Object> serviceMap =
                (Map<String, Object>) streamingCluster.configuration().get("service");
        String brokerUrl = serviceMap.get("serviceUrl").toString();
        appEnv.put("PULSAR_BROKER_URL", brokerUrl);

        deployLocalApplicationAndAwaitReady(
                tenant, applicationId, "python-processor-with-dlq", appEnv, 1);
        String output =
                executeCommandOnClient(
                        "bin/langstream gateway service %s svc -v {\"my-schema\":true} --connect-timeout 60 -p sessionId=s1"
                                .formatted(applicationId)
                                .split(" "));

        log.info("Output: {}", output);
        Assertions.assertTrue(output.contains("Response: 400"));
        Assertions.assertTrue(output.contains("record was not ok:"));

        deleteAppAndAwaitCleanup(tenant, applicationId);
    }
}
