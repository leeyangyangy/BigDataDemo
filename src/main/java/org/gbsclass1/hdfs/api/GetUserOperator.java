package org.gbsclass1.hdfs.api;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @Author liyangyang
 * @Date: 2023/06/03 20:32
 * @Package org.gbsclass1.hdfs.api
 * @Version 1.0
 * @Description:
 */
public class GetUserOperator {

    /**
     * @Param: []
     * @return: java.lang.String
     * @Author: liyangyang
     * @Date: 2023/6/3 20:38
     * @Description: 获取用户输入的内容
     */
    public static String append() {
        Display.common_append();
        // 获取用户输入的文件路径和HDFS文件路径
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        try {
            return reader.readLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



}
