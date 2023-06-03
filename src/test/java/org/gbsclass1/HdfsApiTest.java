package org.gbsclass1;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbsclass1.hdfs.api.HdfsApi;
import org.gbsclass1.hdfs.shell.Tools;
import org.gbsclass1.hdfs.stream.MyFSDataInputStream;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

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
    * @Description: 创建或删除目录
    */
    @Test
    public void testCreateAndDelete() throws IOException {
        tools.createAndDelete("/asd/text.txt");
    }


    /**
    * @Param: []
    * @return: void
    * @Author: liyangyang
    * @Date: 2023/6/3 23:52
    * @Description: 创建和删除文件
    */
    @Test
    public void testCreateAndDeleteDir(){
        tools.createAndDeleteDirByUserOperator("/asd");
    }

    /**
    * @Param: []
    * @return: void
    * @Author: liyangyang
    * @Date: 2023/6/4 0:05
    * @Description: 头尾追加文件内容
    */
    @Test
    public void testUserAppendFile() throws IOException {
        tools.appendFileByUserOperator("/sss");
    }

    /**
    * @Param: []
    * @return: void
    * @Author: liyangyang
    * @Date: 2023/6/4 0:05
    * @Description: 删除文件内容
    */
    @Test
    public void testDeleteFile() throws IOException {
        tools.deleteFile("/sss");
    }

    /**
    * @Param: []
    * @return: void
    * @Author: liyangyang
    * @Date: 2023/6/3 23:59
    * @Description: 测试文件移动
    */
    @Test
    public void testCpFile() throws IOException {
        tools.cpFile("/sss","/ss");
    }

    public static void main(String[] args) throws IOException {
//        Tools tools = new Tools();
//        tools.copyFile2Hdfs("d:/it.txt","/sss");

//        File file=new File("D:\\tmp\\it.txt");
//        获取文件名和父路径
//        System.out.println(file.getParent());
//        System.out.println(file.getName());
//        tools.write2LocalFile("/sss","d:/tmp/it.txt");
//        tools.createAndDeleteDirByUserOperator("/asd");
//            tools.appendFileByUserOperator("");
//        tools.appendFileByUserOperator("/sss");
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

    // 任务二
    
    /**
    * @Param: []
    * @return: void
    * @Author: liyangyang
    * @Date: 2023/6/4 0:37
    * @Description: 编程实现一个类“MyFSDataInputStream”，该类继承“org. apache.hadoop.fs. FSData Input Stream" ,
     * 要求如下:实现按行读取HDFS中指定文件的方法“readLine0",如果读到文件末尾，则返回空，否则返回文件- -行的文本。
    */
    @Test
    public void testReadLine0() throws IOException {

    }


}
