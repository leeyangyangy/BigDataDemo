package org.gbsclass1.hdfs.stream;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @Author liyangyang
 * @Date: 2023/06/04 1:27
 * @Package org.gbsclass1.hdfs.stream
 * @Version 1.0
 * @Description:实现缓存功能，即利用“MyFSDataInputStream” 读取若干字节数据时，
 * 首先查找缓存，如果缓存中有所需的数据，则直接由缓存提供，否则向HDFS读取数据。
 */
public class MyFSDataInputStreamCache extends FSDataInputStream {
    private static final int BUFFER_SIZE = 1024;
    private Map<Long, byte[]> cache = new HashMap<>();
    private long pos = 0;

    public MyFSDataInputStreamCache(FileSystem fs, Path file) throws IOException {
        super(fs.open(file));
    }


    public synchronized int readBytes(byte[] b, int off, int len) throws IOException {
        int bytesRead = 0;
        int totalBytesRead = 0;

        while (len > 0) {
            long cacheKey = pos / BUFFER_SIZE;
            byte[] cacheValue = cache.get(cacheKey);

            if (cacheValue != null) {
                int cachePos = (int) (pos % BUFFER_SIZE);
                int bytesToRead = Math.min(len, cacheValue.length - cachePos);
                System.arraycopy(cacheValue, cachePos, b, off, bytesToRead);
                bytesRead = bytesToRead;
            } else {
                bytesRead = super.read(b, off, len);
                if (bytesRead > 0) {
                    cacheValue = new byte[BUFFER_SIZE];
                    System.arraycopy(b, off, cacheValue, 0, bytesRead);
                    cache.put(cacheKey, cacheValue);
                }
            }

            if (bytesRead == -1) {
                break;
            }

            pos += bytesRead;
            off += bytesRead;
            len -= bytesRead;
            totalBytesRead += bytesRead;
        }

        return totalBytesRead > 0 ? totalBytesRead : -1;
    }

    public synchronized String readLine0() throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        byte[] buffer = new byte[1024];
        while ((c = readBytes(buffer, 0, buffer.length)) != -1) {
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

        // 创建Hadoop配置对象
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:8020/"); // 设置HDFS的URI

        // 创建HDFS文件系统对象
        FileSystem fs = FileSystem.get(conf);

        // 获取要读取的文件路径
        Path path = new Path("/ss");

        // 创建MyFSDataInputStream对象
        MyFSDataInputStream in = new MyFSDataInputStream(fs, path);

        // 输出文件内容到终端
        String line;
        while ((line = in.readLine0()) != null) {
            System.out.println(line);
        }

        // 关闭输入流和文件系统对象
        in.close();
        fs.close();
    }

}
