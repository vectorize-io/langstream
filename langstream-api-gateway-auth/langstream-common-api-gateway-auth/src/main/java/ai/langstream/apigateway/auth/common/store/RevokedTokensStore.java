package ai.langstream.apigateway.auth.common.store;

public interface RevokedTokensStore extends AutoCloseable {

    void refreshRevokedTokens();

    boolean isTokenRevoked(String token);
}
