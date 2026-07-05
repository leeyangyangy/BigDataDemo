package xyz.leeyangy.spc.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "cockpit.wecom")
public class WeComProperties {

    private String corpId = "YOUR_CORP_ID";
    private String agentId = "YOUR_AGENT_ID";
    private String secret = "YOUR_SECRET";
    private boolean enabled = false;
}
