package org.gbsclass1.hdfs.stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.InputStream;

/**
 * @Author liyangyang
 * @Date: 2023/06/04 0:34
 * @Package org.gbsclass1.hdfs.stream
 * @Version 1.0
 * @Description:
 */
public class MyFSDataInputStream extends FSDataInputStream {
    public MyFSDataInputStream(FileSystem fs, Path file) throws IOException {
        super(fs.open(file));
    }

    public String readLine0() throws IOException {
        StringBuilder sb = new StringBuilder();
//        String readLine = new BufferedReader(new InputStreamReader(System.in, "UTF-8")).readLine();
        int c;
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
}
