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

import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.api.util.ObjectMapperFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class JwtAuthenticationProviderConfigurationTest {

    protected static final ObjectMapper yamlConfigReader = ObjectMapperFactory.getYamlMapper();

    @Test
    void parseCamelCase() {
        final String yaml =
                """
                adminRoles:
                    - admin
                    - super-admin
                audience: a
                audienceClaim: aud
                authClaim: role
                publicAlg: RS256
                publicKey: --key--
                secretKey: --skey--
                jwksHostsAllowlist: https://localhost

                                """;
        final JwtAuthenticationProviderConfiguration tokenProperties = parse(yaml);
        assertEquals(List.of("admin", "super-admin"), tokenProperties.adminRoles());
        assertEquals("a", tokenProperties.audience());
        assertEquals("aud", tokenProperties.audienceClaim());
        assertEquals("role", tokenProperties.authClaim());
        assertEquals("RS256", tokenProperties.publicAlg());
        assertEquals("--key--", tokenProperties.publicKey());
        assertEquals("https://localhost", tokenProperties.jwksHostsAllowlist());
        assertEquals("--skey--", tokenProperties.secretKey());
    }

    @Test
    void parseKebabCase() {
        final String yaml =
                """
                admin-roles:
                    - admin
                    - super-admin
                audience: a
                audience-claim: aud
                auth-claim: role
                public-alg: RS256
                public-key: --key--
                secret-key: --skey--
                jwks-hosts-allowlist: https://localhost

                                """;
        final JwtAuthenticationProviderConfiguration tokenProperties = parse(yaml);
        assertEquals(List.of("admin", "super-admin"), tokenProperties.adminRoles());
        assertEquals("a", tokenProperties.audience());
        assertEquals("aud", tokenProperties.audienceClaim());
        assertEquals("role", tokenProperties.authClaim());
        assertEquals("RS256", tokenProperties.publicAlg());
        assertEquals("--key--", tokenProperties.publicKey());
        assertEquals("https://localhost", tokenProperties.jwksHostsAllowlist());
        assertEquals("--skey--", tokenProperties.secretKey());
    }

    @SneakyThrows
    private static JwtAuthenticationProviderConfiguration parse(String yaml) {
        Map map = yamlConfigReader.readValue(yaml, Map.class);
        final JwtAuthenticationProviderConfiguration tokenProperties =
                yamlConfigReader.convertValue(map, JwtAuthenticationProviderConfiguration.class);
        return tokenProperties;
    }
}
