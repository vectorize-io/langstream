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
package ai.langstream.apigateway.auth.impl.jwt.admin;

import ai.langstream.apigateway.auth.common.store.RevokedTokensStoreConfiguration;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record JwtAuthenticationProviderConfiguration(
        @JsonAlias("secret-key") String secretKey,
        @JsonAlias("public-key") String publicKey,
        @JsonAlias("auth-claim") String authClaim,
        @JsonAlias("public-alg") String publicAlg,
        @JsonAlias("audience-claim") String audienceClaim,
        String audience,
        @JsonAlias("admin-roles") List<String> adminRoles,
        @JsonAlias("jwks-hosts-allowlist") String jwksHostsAllowlist,
        @JsonProperty("revoked-tokens-store") RevokedTokensStoreConfiguration revokedTokenStore) {}
