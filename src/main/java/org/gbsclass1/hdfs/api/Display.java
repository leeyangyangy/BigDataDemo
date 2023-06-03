package org.gbsclass1.hdfs.api;

/**
 * @Author liyangyang
 * @Date: 2023/06/02 15:09
 * @Package org.gbsclass1.hdfs.api
 * @Version 1.0
 * @Description: 操作信息显示
 */
public interface Display {
    /**
     * @Param: [localPath, remotePath]
     * @return: void
     * @Author: liyangyang
     * @Date: 2023/6/3 16:19
     * @Description: 上传文件成功
     */
    //    success method
    static void success(String localPath, String remotePath) {
        System.out.println("将本地文件" + localPath + "复制到hdfs路径" + remotePath
                + "上成功");
    }

    /**
     * @Param: [localPath, remotePath]
     * @return: void
     * @Author: liyangyang
     * @Date: 2023/6/3 16:19
     * @Description: 上传文件失败
     */
    //    error method
    static void error(String localPath, String remotePath) {
        System.out.println("将本地文件" + localPath + "复制到hdfs路径" + remotePath
                + "上失败");
    }


    /**
     * @Param: [remotePath, localPath]
     * @return: void
     * @Author: liyangyang
     * @Date: 2023/6/3 16:20
     * @Description: 下在文件成功
     */
    static void download_success(String remotePath, String localPath) {
        System.out.println("从远端文件" + remotePath + "下载成本地文件" + localPath + "完毕");
    }

    /**
     * @Param: [remotePath, localPath]
     * @return: void
     * @Author: liyangyang
     * @Date: 2023/6/3 16:20
     * @Description: 下载失败
     */
    static void download_error(String remotePath, String localPath) {
        System.out.println("从远端文件" + remotePath + "下载成本地文件" + localPath + "失败");
    }

    /**
     * @Param: [remotePath]
     * @return: void
     * @Author: liyangyang
     * @Date: 2023/6/3 16:20
     * @Description: 船舰目录成功
     */
    static void create_dir_success(String remotePath) {
        System.out.println("创建" + remotePath + "目录成功");
    }

    /**
     * @Param: [remotePath]
     * @return: void
     * @Author: liyangyang
     * @Date: 2023/6/3 16:20
     * @Description: 船舰目录失败
     */
    static void create_dir_error(String remotePath) {
        System.out.println("创建" + remotePath + "目录失败");
    }

    /**
     * @Param: [remotePath]
     * @return: void
     * @Author: liyangyang
     * @Date: 2023/6/3 16:20
     * @Description: 删除文件成功
     */
    static void delete_file_success(String remotePath) {
        System.out.println("删除文件"+remotePath+"成功");
    }

    /**
     * @Param: [remotePath]
     * @return: void
     * @Author: liyangyang
     * @Date: 2023/6/3 16:20
     * @Description: 删除文件失败
     */
    static void delete_file_error(String remotePath) {
        System.out.println("删除文件"+remotePath+"失败");
    }

}
