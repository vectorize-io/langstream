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
package ai.langstream.apigateway.auth.common.store;

import java.util.Timer;
import java.util.TimerTask;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RevokedTokensAwareAuthenticationProvider implements AutoCloseable {

    protected RevokedTokensStore revokedTokesStore;

    private Timer timer;

    public void startSyncRevokedTokens(RevokedTokensStoreConfiguration configuration) {
        if (configuration != null) {

            switch (configuration.type()) {
                case "s3":
                    revokedTokesStore = new S3RevokedTokensStore(configuration.configuration());
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unknown revoked token store type: " + configuration.type());
            }

            timer = new Timer();
            final int refreshPeriodSeconds =
                    configuration.refreshPeriod() <= 0 ? 30 : configuration.refreshPeriod();
            timer.scheduleAtFixedRate(
                    new TimerTask() {
                        @Override
                        public void run() {
                            try {
                                revokedTokesStore.refreshRevokedTokens();
                                onRevokedTokensRefreshed();
                            } catch (Throwable tt) {
                                log.error("Error refreshing revoked tokens", tt);
                            }
                        }
                    },
                    0,
                    refreshPeriodSeconds * 1000L);
        }
    }

    // for testing
    public void onRevokedTokensRefreshed() {}

    @Override
    public void close() throws Exception {
        if (timer != null) {
            timer.cancel();
        }
        if (revokedTokesStore != null) {
            revokedTokesStore.close();
        }
    }
}
