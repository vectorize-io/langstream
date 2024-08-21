package ai.langstream.apigateway.auth.impl.jwt.admin;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class JwtAuthenticationProviderConfigurationTest {

    protected static final ObjectMapper yamlConfigReader = new ObjectMapper(new YAMLFactory());

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
