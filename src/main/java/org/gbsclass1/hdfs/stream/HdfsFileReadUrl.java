package org.gbsclass1.hdfs.stream;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

/**
 * @Author liyangyang
 * @Date: 2023/06/04 1:52
 * @Package org.gbsclass1.hdfs.stream
 * @Version 1.0
 * @Description: 查看Java帮助手册或其它资料，
 * 用“java.net.URL” 和“orgapache.hadoop.fs.FsURLStreamHandlerFactory”编程完成输出HDFS中指定文件的文本到终端中。
 */
public class HdfsFileReadUrl {
    public static void main(String[] args) throws Exception {

        // 注册HDFS URL流处理工厂
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

        // 创建Hadoop配置对象
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000"); // 设置HDFS的URI

        // 创建HDFS文件系统对象
        FileSystem fs = FileSystem.get(conf);

        // 获取要读取的文件路径
        Path path = new Path("/ss");

        // 创建URL对象
        URL url = new URL("hdfs://master:8020/" + path.toUri().getPath());

        // 打开URL连接
        BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()));

        // 输出文件内容到终端
        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }

        // 关闭连接和文件系统对象
        br.close();
        fs.close();
    }
}
