package org.gbsclass1;

import org.gbsclass1.hdfs.shell.Tools;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

/**
 * @Author liyangyang
 * @Date: 2023/06/02 15:39
 * @Package org.gbsclass1
 * @Version 1.0
 * @Description:
 */
public class HdfsApiTest {



    @Test
    public void copyFile2Hdfs() throws IOException {
        Tools tools = new Tools();
        tools.copyFile2Hdfs("d:/it.txt","/sss");
//        Display.success("1","2");
    }


    public static void main(String[] args) throws IOException {
        Tools tools = new Tools();
//        tools.copyFile2Hdfs("d:/it.txt","/sss");

//        File file=new File("D:\\tmp\\it.txt");
//        获取文件名和父路径
//        System.out.println(file.getParent());
//        System.out.println(file.getName());
        tools.write2LocalFile("/sss","d:/tmp/it.txt");
    }



}
