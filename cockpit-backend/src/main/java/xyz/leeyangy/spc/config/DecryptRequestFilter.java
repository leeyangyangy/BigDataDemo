package xyz.leeyangy.spc.config;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import xyz.leeyangy.spc.common.AESUtil;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Component
@Order(1)
public class DecryptRequestFilter implements Filter {

    // 用 Spring 注入的 ObjectMapper, 自带 JavaTimeModule 支持。
    private final ObjectMapper objectMapper;
    private final AESUtil aesUtil;

    @Autowired
    public DecryptRequestFilter(ObjectMapper objectMapper, AESUtil aesUtil) {
        this.objectMapper = objectMapper;
        this.aesUtil = aesUtil;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        if (!aesUtil.isEncryptionEnabled()) {
            chain.doFilter(request, response);
            return;
        }

        HttpServletRequest httpRequest = (HttpServletRequest) request;
        String requestPath = httpRequest.getRequestURI();

        if (aesUtil.isPathExcluded(requestPath)) {
            chain.doFilter(request, response);
            return;
        }

        String encryptedHeader = httpRequest.getHeader("X-Encrypted");

        if (!"true".equals(encryptedHeader) || !"POST".equalsIgnoreCase(httpRequest.getMethod())
                && !"PUT".equalsIgnoreCase(httpRequest.getMethod())) {
            chain.doFilter(request, response);
            return;
        }

        String contentType = httpRequest.getContentType();
        if (contentType == null || !contentType.contains("application/json")) {
            chain.doFilter(request, response);
            return;
        }

        String body = readBody(httpRequest);
        if (body == null || body.isEmpty()) {
            chain.doFilter(request, response);
            return;
        }

        // 读取过 body 后原始 InputStream 已被消费,
        // 必须用 wrapper 重新包装 (无论是否解密), 否则下游 @RequestBody 会抛 Stream closed。
        String finalBody = body;

        // userId 由 JwtAuthFilter 提前写入 request attribute;
        // 若拿不到 (如未登录), 跳过解密, 但仍需用原始 body 包装请求。
        Long userId = (Long) httpRequest.getAttribute("userId");

        try {
            Map<String, Object> encryptedMap = objectMapper.readValue(body, Map.class);
            boolean isEncrypted = Boolean.TRUE.equals(encryptedMap.get("encrypted"));
            String encryptedData = (String) encryptedMap.get("data");

            if (isEncrypted && encryptedData != null && !encryptedData.isEmpty()) {
                finalBody = aesUtil.decrypt(encryptedData, userId);
            }
        } catch (Exception e) {
            System.err.println("[Crypto] 请求解密失败，使用原始数据: " + e.getMessage());
        }

        HttpServletRequestWrapper wrappedRequest = new DecryptedRequestWrapper(httpRequest, finalBody);
        chain.doFilter(wrappedRequest, response);
    }

    private String readBody(HttpServletRequest request) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(request.getInputStream(), StandardCharsets.UTF_8))) {
            char[] buffer = new char[1024];
            int bytesRead;
            while ((bytesRead = reader.read(buffer)) != -1) {
                sb.append(buffer, 0, bytesRead);
            }
        }
        return sb.toString();
    }

    private static class DecryptedRequestWrapper extends HttpServletRequestWrapper {

        private final String decryptedBody;

        public DecryptedRequestWrapper(HttpServletRequest request, String decryptedBody) {
            super(request);
            this.decryptedBody = decryptedBody;
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            ByteArrayInputStream bais = new ByteArrayInputStream(
                    decryptedBody.getBytes(StandardCharsets.UTF_8));
            return new ServletInputStream() {
                @Override
                public int read() throws IOException {
                    return bais.read();
                }

                @Override
                public boolean isFinished() {
                    return bais.available() == 0;
                }

                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setReadListener(ReadListener listener) {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public BufferedReader getReader() throws IOException {
            return new BufferedReader(new InputStreamReader(
                    getInputStream(), StandardCharsets.UTF_8));
        }
    }
}
