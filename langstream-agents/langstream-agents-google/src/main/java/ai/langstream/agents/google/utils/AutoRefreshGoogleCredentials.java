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
package ai.langstream.agents.google.utils;

import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AutoRefreshGoogleCredentials implements AutoCloseable {

    private final GoogleCredentials credentials;
    private final Timer refreshTokenTimer;

    public AutoRefreshGoogleCredentials(GoogleCredentials credentials) throws IOException {
        this.credentials = credentials;
        refreshTokenTimer = new Timer();
        refreshTokenTimer.scheduleAtFixedRate(
                new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            credentials.refreshIfExpired();
                        } catch (Exception e) {
                            log.error("Error refreshing token", e);
                        }
                    }
                },
                60000,
                60000);
        // let's fail now if something is wrong
        credentials.refreshIfExpired();
    }

    @Override
    public void close() {
        if (refreshTokenTimer != null) {
            refreshTokenTimer.cancel();
        }
    }
}
