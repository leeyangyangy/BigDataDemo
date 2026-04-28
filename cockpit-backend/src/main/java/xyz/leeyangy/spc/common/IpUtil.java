package xyz.leeyangy.spc.common;

import lombok.extern.slf4j.Slf4j;
import javax.servlet.http.HttpServletRequest;
import java.util.regex.Pattern;

@Slf4j
public final class IpUtil {
    private static final Pattern IPV6_MAPPED_IPV4 = Pattern.compile("^::ffff:(\\d+\\.\\d+\\.\\d+\\.\\d+)$", Pattern.CASE_INSENSITIVE);
    private static final String UNKNOWN = "unknown";
    private static final String LOCAL_IP_V6 = "0:0:0:0:0:0:0:1";

    private IpUtil() {}

    public static String getClientIp(HttpServletRequest request) {
        if (request == null) {
            log.warn("[IpUtil] request is null, cannot get client IP");
            return null;
        }

        String ip = null;
        String sourceHeader = null;

        String[] headerNames = {
            "X-Forwarded-For",
            "X-Real-IP",
            "Proxy-Client-IP",
            "WL-Proxy-Client-IP"
        };

        for (String header : headerNames) {
            String headerValue = request.getHeader(header);
            if (isValidIp(headerValue)) {
                ip = headerValue;
                sourceHeader = header;
                break;
            }
        }

        if (!isValidIp(ip)) {
            ip = request.getRemoteAddr();
            sourceHeader = "RemoteAddr";
        }

        String normalizedIp = normalizeIp(ip);

        if ("127.0.0.1".equals(normalizedIp) || "::1".equals(normalizedIp) || "0:0:0:0:0:0:0:1".equals(normalizedIp)) {
            log.warn("[IpUtil] 检测到本地回环地址! URI={} | 来源={} | 原始值={} | 可能原因: 请求经过本地代理(如Vite dev server)未转发真实IP",
                    request.getRequestURI(), sourceHeader, ip);
            log.debug("[IpUtil] 所有请求头: XFF={}, XRI={}, Proxy-Client-IP={}, WL-Proxy-IP={}, RemoteAddr={}",
                    request.getHeader("X-Forwarded-For"),
                    request.getHeader("X-Real-IP"),
                    request.getHeader("Proxy-Client-IP"),
                    request.getHeader("WL-Proxy-Client-IP"),
                    request.getRemoteAddr());
        } else {
            log.debug("[IpUtil] 获取客户端IP成功: {} (来源: {})", normalizedIp, sourceHeader);
        }

        return normalizedIp;
    }

    private static boolean isValidIp(String ip) {
        return ip != null && !ip.isEmpty() && !UNKNOWN.equalsIgnoreCase(ip) && !"null".equals(ip.trim());
    }

    private static String normalizeIp(String ip) {
        if (ip == null || ip.isEmpty()) return ip;

        if (ip.contains(",")) {
            ip = ip.split(",")[0].trim();
        }
        if (LOCAL_IP_V6.equals(ip)) {
            return "127.0.0.1";
        }
        var matcher = IPV6_MAPPED_IPV4.matcher(ip);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return ip;
    }
}