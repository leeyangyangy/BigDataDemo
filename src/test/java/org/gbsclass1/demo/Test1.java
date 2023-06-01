package org.gbsclass1.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class Test1 {
    //    在hdfs中创建文件夹
    public static void method1() {
//        获取hdfs操作对象 FileSystem对象
//        Configuration  获取hdfs对象
        Configuration conf = new Configuration();
//        获取hdfs连接uri 参数位hdfs连接字符串
        URI uri = URI.create("hdfs://192.168.1.7:19000");
        Path path = new Path("/aaa");
        try {
            FileSystem fs = FileSystem.get(uri, conf,"hadoop");
            boolean b = fs.exists(path);
//            判断文件是否存在
            if (false == b) {
//                创建文件夹
                boolean mkdirs = fs.mkdirs(path);
                if (mkdirs) System.out.println("创建目录"+path.toString()+",结果为："+mkdirs);
            }else {
                System.out.println("目录已经存在");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        method1();
    }
}
