package org.gbsclass1.hdfs.shell;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbsclass1.hdfs.api.Display;
import org.gbsclass1.hdfs.api.HdfsApi;

import java.io.*;
import java.util.Scanner;

/**
 * @Author liyangyang
 * @Date: 2023/06/02 13:59
 * @Package org.gbsclass1.hdfs.shell
 * @Version 1.0
 * @Description: 文件上传
 */
public class Tools {

    /**
     * @Param: [localPath, remotePath]
     *          本地文件路径，目标路径
     * @return: void
     * @Author: liyangyang
     * @Date: 2023/6/2 14:04
     * @Description:
     *              向HDFS中上传任意文本文件，如果指定的文件在HDFS中已经存在，
     *              由用户指定的是追加到原有文件末尾还是覆盖原有的文件。
     */
    void copyFile2Hdfs(String localPath, String remotePath) throws IOException {
//        localPath，本地文件路径；remotePath，hdfs上远端文件的路径
        FileSystem fs = HdfsApi.getFS();

        if (localPath==null ){
            System.out.println("请检填写本地文件路径");
            return;
        } else if (remotePath ==null) {
            System.out.println("请填写上传文件路径");
            return;
        } else if(fs!=null){
            // 判断HDFS文件是否已经存在
            if (fs.exists(new Path(remotePath))){
                // 获取文件或文件夹的路径
                Path path = new Path(remotePath);

                // 获取文件或文件夹的元数据信息
                FileStatus status = fs.getFileStatus(path);

                // 判断文件或文件夹的类型
                if (status.isDirectory()) {
                    //  直接上传文件
                    System.out.println(path + " 是文件夹");
                } else if (status.isFile()) {
                    System.out.println(path + " 是文件");
                    System.out.print("文件已经存在，要追加还是覆盖？(A-追加，O-覆盖)：");

                    // 获取用户输入的文件路径和HDFS文件路径
                    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                    String s = reader.readLine();
                    if ("A".equalsIgnoreCase(s)){

                        //  本地文件开输入流，进行读取
                        FileInputStream fis = null;
                        FSDataOutputStream append = null;
                        byte[] buffer = new byte[1024];
                        try {
                            //  新建一个文件输入流，打开本地文件
                            fis = new FileInputStream(new File(localPath));
                            //  远端文件开输出流
                            //  append方法，打开一个已存在的文件
                            append = fs.append(new Path(remotePath));
                            //  进行追加写入，如果文件大小超过字节数组，则需要循环读取并写入
                            while(true) {
                                //  将文件的内容读出，写入一个byte数组
                                int read = fis.read(buffer);
                                if(read < 0) {
                                    //  表示未能从文件读取出任何信息，文件已结束，退出循环
                                    break;
                                }
                                //  如果 read >=0，表示文件未结束，信息已读取到字节数组
                                append.write(buffer, 0, read);
                            }
                            append.flush();
                            Display.success(localPath, remotePath);
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        } finally {
                            if(fis != null) {
                                try {
                                    fis.close();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }

                            if(append != null) {
                                try {
                                    append.close();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }else {
                        // 覆盖原有文件
                        fs.copyFromLocalFile(new Path(localPath), new Path(remotePath));
                        Display.success(localPath, remotePath);
                    }

                } else {
                    //  直接上传文件
                    System.out.println(path + " 不存在");
                    fs.copyFromLocalFile(new Path(localPath), new Path(remotePath));
                }
            }else {
                fs.copyFromLocalFile(new Path(localPath), new Path(remotePath));
                Display.success(localPath, remotePath);
            }
        }else {
            System.out.println("未知错误");
            Display.error(localPath, remotePath);
        }
        // 关闭文件系统对象
        HdfsApi.closeFS(fs);
    }
}
