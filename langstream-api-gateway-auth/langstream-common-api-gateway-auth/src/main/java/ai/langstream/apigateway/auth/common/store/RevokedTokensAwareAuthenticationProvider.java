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
                    revokedTokesStore = new S3RevokedTokensStore(configuration.config());
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
