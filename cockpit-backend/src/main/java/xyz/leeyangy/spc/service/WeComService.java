package xyz.leeyangy.spc.service;

import java.util.Map;

/**
 * 企业微信 Service 接口
 */
public interface WeComService {

    boolean isEnabled();

    String getCorpId();

    String getAgentId();

    String getAccessToken();

    String refreshAccessToken();

    Map<String, Object> getUserInfoByCode(String code);
}
