package xyz.leeyangy.hdfs.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

//操作hdfs对象，生成FileSystem对象
public class HdfsHelper {
//    在hdfs上创建文件夹

    //    私有静态conf对象
    private static Configuration _conf = null;

    //    连接hdfs对象
    private static URI _hdfsUri = null;

    private static String _username = "lee";

    //    静态初始化方法
    static {
        _conf = new Configuration();
        _hdfsUri = URI.create("hdfs://192.168.1.7:19000");
    }

    public static FileSystem getFS() {
//        根据指定参数，生成并返回FileSystem对象
        try {
            FileSystem fileSystem = FileSystem.get(_hdfsUri, _conf, _username);
            return fileSystem;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
