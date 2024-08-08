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
