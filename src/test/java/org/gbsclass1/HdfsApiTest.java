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


    Tools tools = new Tools();

    /**
    * @Param: []
    * @return: void
    * @Author: liyangyang
    * @Date: 2023/6/3 15:25
    * @Description: 文件上传
    */
    @Test
    public void copyFile2Hdfs() throws IOException {
        tools = new Tools();
        tools.copyFile2Hdfs("d:/it.txt","/sss");
//        Display.success("1","2");
    }

    /**
     * @Param: []
     * @return: void
     * @Author: liyangyang
     * @Date: 2023/6/3 15:25
     * @Description: 文件下载
     */
    @Test
    public void testDownloadFIle() throws IOException {
        tools.write2LocalFile("/sss","d:/tmp/it.txt");
    }

    /**
    * @Param: []
    * @return: void
    * @Author: liyangyang
    * @Date: 2023/6/3 15:25
    * @Description: 文件信息读取
    */
    @Test
    public void testReadFile() throws IOException {
        tools.getFileContent("/sss");
    }

    /**
    * @Param: []
    * @return: void
    * @Author: liyangyang
    * @Date: 2023/6/3 15:39
    * @Description: 获取文件信息
    */
    @Test
    public void testGetFileInfo() throws IOException {
        tools.getFIleInfo("/sss");
        System.out.println("--------------");
        tools.getFIleInfo("/6");
    }

    /**
    * @Param: []
    * @return: void
    * @Author: liyangyang
    * @Date: 2023/6/3 15:40
    * @Description: 遍历所有文件信息
    */
    @Test
    public void testListDir(){
        tools.listFiles("/spark_input");
        System.out.println("--------------");
        tools.listFiles("/user");
    }

    /**
    * @Param: []
    * @return: void
    * @Author: liyangyang
    * @Date: 2023/6/3 17:47
    * @Description:
    */
    @Test
    public void testCreateAndDelete() throws IOException {
        tools.createAndDelete("/asd/text.txt");
    }


    @Test
    public void testCreateAndDeleteDir(){
        tools.createAndDeleteDirByUserOperator("/asd");
    }

    public static void main(String[] args) throws IOException {
        Tools tools = new Tools();
        tools.copyFile2Hdfs("d:/it.txt","/sss");

//        File file=new File("D:\\tmp\\it.txt");
//        获取文件名和父路径
//        System.out.println(file.getParent());
//        System.out.println(file.getName());
//        tools.write2LocalFile("/sss","d:/tmp/it.txt");
//        tools.createAndDeleteDirByUserOperator("/asd");
//            tools.appendFileByUserOperator("");
        tools.appendFileByUserOperator("/sss");
    }



}
