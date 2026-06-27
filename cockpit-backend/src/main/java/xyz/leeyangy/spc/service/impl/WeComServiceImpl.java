package xyz.leeyangy.spc.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import xyz.leeyangy.spc.common.exception.BusinessException;
import xyz.leeyangy.spc.common.exception.ExternalServiceException;
import xyz.leeyangy.spc.common.exception.ParamValidationException;
import xyz.leeyangy.spc.config.WeComProperties;
import xyz.leeyangy.spc.service.WeComService;

import java.time.Duration;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class WeComServiceImpl implements WeComService {

    private final WeComProperties properties;
    private final StringRedisTemplate redisTemplate;
    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String ACCESS_TOKEN_KEY = "wecom:access_token";
    private static final String WECOM_TOKEN_URL = "https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=%s&corpsecret=%s";
    private static final String WECOM_USER_INFO_URL = "https://qyapi.weixin.qq.com/cgi-bin/user/getuserinfo?access_token=%s&code=%s";
    private static final String SERVICE_NAME = "WeCom";

    @Override
    public boolean isEnabled() {
        return properties.isEnabled();
    }

    @Override
    public String getCorpId() {
        return properties.getCorpId();
    }

    @Override
    public String getAgentId() {
        return properties.getAgentId();
    }

    @Override
    public String getAccessToken() {
        String cached = redisTemplate.opsForValue().get(ACCESS_TOKEN_KEY);
        if (cached != null) {
            return cached;
        }
        return refreshAccessToken();
    }

    @Override
    public String refreshAccessToken() {
        String url = String.format(WECOM_TOKEN_URL, properties.getCorpId(), properties.getSecret());
        try {
            ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
            Map<String, Object> result = objectMapper.readValue(response.getBody(), new TypeReference<Map<String, Object>>() {});

            int errcode = ((Number) result.get("errcode")).intValue();
            if (errcode != 0) {
                log.error("[WeCom] 获取access_token失败: errcode={} errmsg={}", errcode, result.get("errmsg"));
                throw new ExternalServiceException(SERVICE_NAME, "获取access_token失败: " + result.get("errmsg"));
            }

            String token = (String) result.get("access_token");
            int expiresIn = ((Number) result.get("expires_in")).intValue();

            redisTemplate.opsForValue().set(ACCESS_TOKEN_KEY, token, Duration.ofSeconds(expiresIn - 300));
            log.info("[WeCom] 刷新access_token成功");
            return token;
        } catch (ExternalServiceException e) {
            throw e;
        } catch (Exception e) {
            log.error("[WeCom] 刷新access_token异常", e);
            throw new ExternalServiceException(SERVICE_NAME, "获取凭证失败", e);
        }
    }

    @Override
    public Map<String, Object> getUserInfoByCode(String code) {
        String accessToken = getAccessToken();
        String url = String.format(WECOM_USER_INFO_URL, accessToken, code);

        try {
            ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
            Map<String, Object> result = objectMapper.readValue(response.getBody(), new TypeReference<Map<String, Object>>() {});

            int errcode = ((Number) result.get("errcode")).intValue();
            if (errcode != 0) {
                if (errcode == 40029) {
                    throw new ParamValidationException("扫码已过期，请重新扫码");
                }
                log.error("[WeCom] 获取用户信息失败: errcode={} errmsg={}", errcode, result.get("errmsg"));
                throw new ExternalServiceException(SERVICE_NAME, "获取用户信息失败: " + result.get("errmsg"));
            }

            log.info("[WeCom] 通过code获取用户信息成功: userId={}", result.get("UserId"));
            return result;
        } catch (BusinessException e) {
            throw e;
        } catch (Exception e) {
            log.error("[WeCom] 获取用户信息异常", e);
            throw new ExternalServiceException(SERVICE_NAME, "获取用户信息失败", e);
        }
    }
}
