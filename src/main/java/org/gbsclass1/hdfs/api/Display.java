package org.gbsclass1.hdfs.api;

/**
 * @Author liyangyang
 * @Date: 2023/06/02 15:09
 * @Package org.gbsclass1.hdfs.api
 * @Version 1.0
 * @Description: 操作信息显示
 */
public interface Display {
    //    success method
    static void success(String localPath, String remotePath) {
        System.out.println("将本地文件" + localPath + "复制到hdfs路径" + remotePath
                + "上成功");
    }

    //    error method
    static void error(String localPath, String remotePath) {
        System.out.println("将本地文件" + localPath + "复制到hdfs路径" + remotePath
                + "上失败");
    }

    static void download_success(String remotePath, String localPath) {
        System.out.println("从远端文件" + remotePath + "下载成本地文件" + localPath + "完毕");
    }


}
