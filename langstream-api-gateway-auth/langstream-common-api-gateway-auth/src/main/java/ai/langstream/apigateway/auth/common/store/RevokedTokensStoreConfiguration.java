package ai.langstream.apigateway.auth.common.store;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public record RevokedTokensStoreConfiguration(
        String type,
        @JsonProperty("refresh-period-seconds") int refreshPeriod,
        Map<String, Object> configuration) {}
