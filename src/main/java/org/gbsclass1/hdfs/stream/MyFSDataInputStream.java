package org.gbsclass1.hdfs.stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbsclass1.hdfs.api.HdfsApi;

import java.io.InputStream;

/**
 * @Author liyangyang
 * @Date: 2023/06/04 0:34
 * @Package org.gbsclass1.hdfs.stream
 * @Version 1.0
 * @Description: 实现按行读取HDFS中指定文件的方法“readLine0",如果读到文件末尾，则返回为空，否则返回文件一行文本。
 */
public class MyFSDataInputStream extends FSDataInputStream {
    public MyFSDataInputStream(FileSystem fs, Path file) throws IOException {
        super(fs.open(file));
    }

    public String readLine0() throws IOException {
        StringBuilder sb = new StringBuilder();
//        String readLine = new BufferedReader(new InputStreamReader(System.in, "UTF-8")).readLine();
        //  读入数据记录
        int c;
        //  实现按行读取HDFS中指定文件的方法`readLine0()
        while ((c = read()) != -1) {
            if (c == '\n') {
                break;
            } else if (c != '\r') {
                sb.append((char) c);
            }
        }

        if (sb.length() == 0 && c == -1) {
            return null;
        }

        return sb.toString();
    }

    public static void main(String[] args) throws IOException {
        //  创建文件对象
        FileSystem fs = HdfsApi.getFS();
        Path path = new Path("/ss");
        MyFSDataInputStream myfsdInput = new MyFSDataInputStream(fs,path);
        String line;
        while ((line = myfsdInput.readLine0()) != null) {
            System.out.println(line);
        }
        // 关闭输入流和文件系统对象
        myfsdInput.close();
        HdfsApi.closeFS(fs);
    }

}
