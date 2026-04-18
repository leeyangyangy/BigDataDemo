package xyz.leeyangy.spc.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.MethodParameter;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;
import xyz.leeyangy.spc.common.AESUtil;

import java.util.Map;

@ControllerAdvice
public class EncryptResponseAdvice implements ResponseBodyAdvice<Object> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AESUtil aesUtil;

    @Autowired
    public EncryptResponseAdvice(AESUtil aesUtil) {
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

        try {
            String jsonStr;
            if (body instanceof String) {
                jsonStr = (String) body;
            } else {
                jsonStr = objectMapper.writeValueAsString(body);
            }

            String encryptedData = aesUtil.encrypt(jsonStr);

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
