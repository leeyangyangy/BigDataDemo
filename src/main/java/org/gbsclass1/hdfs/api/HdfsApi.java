package org.gbsclass1.hdfs.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * @Author liyangyang
 * @Date: 2023/06/02 13:18
 * @Package org.gbsclass1.hdfs.api
 * @Version 1.0
 * @Description: api 工具类，简化连接Hadoop过程
 */
public class HdfsApi {

    //    私有静态conf对象
    private static Configuration _conf = null;

    //    连接hdfs对象
    private static URI _hdfsUri = null;

    private static String _username = "hadoop";

    //    静态初始化方法
    static {
        _conf = new Configuration();
        _hdfsUri = URI.create("hdfs://master:8020");
        _conf.set("dfs.client.use.datanode.hostname", "true");
    }

    public static FileSystem getFS() {
        //  根据指定参数，生成并返回FileSystem对象
        try {
            FileSystem fileSystem = FileSystem.get(_hdfsUri, _conf, _username);
            return fileSystem;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    //    关闭 hdfs 对象，释放资源
    public static void closeFS(FileSystem fs) {
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
