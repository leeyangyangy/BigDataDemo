package xyz.leeyangy.spc.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@EnableConfigurationProperties
@Import({DecryptRequestFilter.class, EncryptResponseAdvice.class})
@ConditionalOnProperty(name = "cockpit.security.encryption.enabled", havingValue = "true", matchIfMissing = false)
public class EncryptionConfig {
}
