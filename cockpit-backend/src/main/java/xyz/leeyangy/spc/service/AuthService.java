package xyz.leeyangy.spc.service;

import java.util.Map;

public interface AuthService {

    Map<String, Object> login(String empNo, String password, String ip);

    Map<String, Object> wechatLogin(String code, Map<String, Object> userInfo, String ip);
}