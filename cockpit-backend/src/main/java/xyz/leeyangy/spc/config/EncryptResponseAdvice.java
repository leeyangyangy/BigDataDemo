package xyz.leeyangy.spc.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.MethodParameter;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;
import xyz.leeyangy.spc.common.AESUtil;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

@ControllerAdvice
public class EncryptResponseAdvice implements ResponseBodyAdvice<Object> {

    // 必须用 Spring 容器注入的 ObjectMapper, 它已自动注册 JavaTimeModule
    // 并应用 application.yml 中的 date-format / time-zone 配置;
    // new ObjectMapper() 不支持 LocalDateTime, 会导致响应加密失败。
    private final ObjectMapper objectMapper;
    private final AESUtil aesUtil;

    @Autowired
    public EncryptResponseAdvice(ObjectMapper objectMapper, AESUtil aesUtil) {
        this.objectMapper = objectMapper;
        this.aesUtil = aesUtil;
    }

    @Override
    public boolean supports(MethodParameter returnType,
                            Class<? extends HttpMessageConverter<?>> converterType) {
        return aesUtil.isEncryptionEnabled();
    }

    @Override
    public Object beforeBodyWrite(Object body, MethodParameter returnType,
                                  MediaType selectedContentType,
                                  Class<? extends HttpMessageConverter<?>> selectedConverterType,
                                  ServerHttpRequest request, ServerHttpResponse response) {

        if (!aesUtil.isEncryptionEnabled() || body == null) {
            return body;
        }

        String requestPath = request.getURI().getPath();
        if (aesUtil.isPathExcluded(requestPath)) {
            return body;
        }

        // 从原生 HttpServletRequest 取 userId (JwtAuthFilter 已写入)。
        // 拿不到时直接返回原文, 避免登录/公开接口报错。
        Long userId = null;
        if (request instanceof ServletServerHttpRequest) {
            HttpServletRequest servletRequest =
                    ((ServletServerHttpRequest) request).getServletRequest();
            Object attr = servletRequest.getAttribute("userId");
            if (attr instanceof Long) {
                userId = (Long) attr;
            }
        }
        if (userId == null) {
            return body;
        }

        try {
            String jsonStr;
            if (body instanceof String) {
                jsonStr = (String) body;
            } else {
                jsonStr = objectMapper.writeValueAsString(body);
            }

            String encryptedData = aesUtil.encrypt(jsonStr, userId);

            // KV 不存在时 encrypt 返回原文, 此时不应包成 encrypted envelope, 否则前端无法解密
            if (encryptedData == jsonStr || encryptedData.equals(jsonStr)) {
                return body;
            }

            Map<String, Object> result = new java.util.HashMap<>();
            result.put("encrypted", true);
            result.put("data", encryptedData);

            response.getHeaders().set("X-Encrypted-Response", "true");

            return result;
        } catch (Exception e) {
            System.err.println("[Crypto] 响应加密失败，返回原始数据: " + e.getMessage());
            return body;
        }
    }
}
