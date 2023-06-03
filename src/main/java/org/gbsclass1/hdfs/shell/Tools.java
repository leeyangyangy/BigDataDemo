package org.gbsclass1.hdfs.shell;

import org.apache.hadoop.fs.*;
import org.gbsclass1.hdfs.api.Display;
import org.gbsclass1.hdfs.api.GetUserOperator;
import org.gbsclass1.hdfs.api.HdfsApi;

import java.io.*;
import java.util.Scanner;
import java.util.UUID;

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
     * 本地文件路径，目标路径
     * @return: void
     * @Author: liyangyang
     * @Date: 2023/6/2 14:04
     * @Description: 向HDFS中上传任意文本文件，如果指定的文件在HDFS中已经存在，
     * 由用户指定的是追加到原有文件末尾还是覆盖原有的文件。
     */
    public void copyFile2Hdfs(String localPath, String remotePath) throws IOException {
//        localPath，本地文件路径；remotePath，hdfs上远端文件的路径
        FileSystem fs = HdfsApi.getFS();

        if (localPath == null) {
            System.out.println("请填写本地文件路径");
            return;
        } else if (remotePath == null) {
            System.out.println("请填写上传文件路径");
            return;
        } else if (fs != null) {
            // 判断HDFS文件是否已经存在
            if (fs.exists(new Path(remotePath))) {
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
                    if ("A".equalsIgnoreCase(s)) {

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
                            while (true) {
                                //  将文件的内容读出，写入一个byte数组
                                int read = fis.read(buffer);
                                if (read < 0) {
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
                            if (fis != null) {
                                try {
                                    fis.close();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }

                            if (append != null) {
                                try {
                                    append.close();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    } else {
                        // 覆盖原有文件
                        fs.copyFromLocalFile(new Path(localPath), new Path(remotePath));
                        Display.success(localPath, remotePath);
                    }

                } else {
                    //  直接上传文件
                    System.out.println(path + " 不存在");
                    fs.copyFromLocalFile(new Path(localPath), new Path(remotePath));
                }
            } else {
                fs.copyFromLocalFile(new Path(localPath), new Path(remotePath));
                Display.success(localPath, remotePath);
            }
        } else {
            System.out.println("未知错误");
            Display.error(localPath, remotePath);
        }
        // 关闭文件系统对象
        HdfsApi.closeFS(fs);
    }


    /**
     * @Param: [remotePath, localPath]
     * @return: void
     * @Author: liyangyang
     * @Date: 2023/6/2 15:50
     * @Description: 从HDFS中下载指定文件，如果本地文件与要下载的文件名称相同，则自动对下载的文件重命名
     */
    public void write2LocalFile(String remotePath, String localPath) throws IOException {
        //  用文件流的方式，从远端下载文件到本地
        //  localPath，本地文件路径；remotePath，hdfs上远端文件的路径
        FileSystem fs = HdfsApi.getFS();
        if (localPath == null) {
            System.out.println("请填写本地文件路径");
            return;
        } else if (remotePath == null) {
            System.out.println("请填写下载文件路径");
            return;
        } else if (fs != null) {
            //  判断远程文件是否存在
            if (fs.exists(new Path(remotePath))) {
                File localfile = new File(localPath);
                //  执行下载
                //  远端文件开输入流，进行读取
                FSDataInputStream fsInput = null;
                //  本地文件开输出，进行写入
                FileOutputStream fos = null;
                //  缓冲区
                byte[] buffer = new byte[1024];

                // 如果本地文件存在
                if (localfile.exists()) {
                    //  修改文件名
                    System.out.println(localfile.getParent());
                    String parentSrc = localfile.getParent();
                    //  拼接路径
                    String newSrc = parentSrc + "\\" + UUID.randomUUID() + "-" + localfile.getName();
                    try {
                        //  打开远端文件，以便读取
                        fsInput = fs.open(new Path(remotePath));
                        //  打开本地文件，以便输出
                        fos = new FileOutputStream(newSrc);
                        //  开始循环读取写入
                        while (true) {
                            //  本轮循环，读取到内容，返回值为读取的字节数
                            int read = fsInput.read(buffer);
                            if (read < 0) {
                                //  read = -1，表示文件已经读取完毕，退出循环
                                break;
                            }
//                            System.out.print(new String(buffer));
                            fos.write(buffer, 0, read);
                        }
                        Display.download_success(remotePath, newSrc);
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        if (fos != null) {
                            try {
                                fos.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }

                        if (fsInput != null) {
                            try {
                                fsInput.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                } else {
                    try {
                        //  打开远端文件，以便读取
                        fsInput = fs.open(new Path(remotePath));
                        //  打开本地文件，以便输出
                        fos = new FileOutputStream(new File(localPath));
                        //  开始循环读取写入
                        while (true) {
                            //  本轮循环，读取到内容，返回值为读取的字节数
                            int read = fsInput.read(buffer);
                            if (read < 0) {
                                //  read = -1，表示文件已经读取完毕，退出循环
                                break;
                            }
//                            System.out.print(new String(buffer));
                            fos.write(buffer, 0, read);
                        }
                        Display.download_success(remotePath, localPath);
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        if (fos != null) {
                            try {
                                fos.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }

                        if (fsInput != null) {
                            try {
                                fsInput.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            } else {
                System.out.println("远程文件不存在");
            }
        }
        HdfsApi.closeFS(fs);

    }

    /**
     * @Param: [remotePath]
     * @return: void
     * @Author: liyangyang
     * @Date: 2023/6/3 15:00
     * @Description: 将HDFS中指定文件的内容输出到终端。
     */
    public void getFileContent(String remotePath) throws IOException {
//        获取远程文件对象
        FileSystem fs = HdfsApi.getFS();
        Path path = new Path(remotePath);
        try {
            if (fs.exists(path)) {
                // 打开文件
                FSDataInputStream fsInput = fs.open(path);

                //  缓冲区
                byte[] buffer = new byte[1024];

                //  开始循环读取写入
                while (true) {
                    //  本轮循环，读取到内容，返回值为读取的字节数
                    int read = fsInput.read(buffer);
                    if (read < 0) {
                        //  read = -1，表示文件已经读取完毕，退出循环
                        break;
                    }
                    System.out.print(new String(buffer));
                }
                //  关闭
                fsInput.close();
            } else {
                System.out.println("远程文件不存在");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //  关闭hdfs
            HdfsApi.closeFS(fs);
        }

    }


    /**
     * @Param: [remotePath]
     * @return: void
     * @Author: liyangyang
     * @Date: 2023/6/3 15:20
     * @Description: 显示HDFS中指定的文件的读写权限、大小、创建时间、路径等信息
     */
    public void getFIleInfo(String remotePath) throws IOException {
//        获取文件对象
        FileSystem fs = HdfsApi.getFS();
//        路径非空判断
        if (remotePath == null) {
            System.out.println("文件输入路径为空，请重试");
            return;
        }
        Path path = new Path(remotePath);
        try {
            //  判断远程文件是否为空
            if (fs.exists(path)) {
                // 获取文件状态
                FileStatus status = fs.getFileStatus(path);
                // 显示读写权限
                System.out.println("Permission: " + status.getPermission());
                // 显示大小
                System.out.println("Size: " + status.getLen());
                // 显示创建时间
                System.out.println("Creation time: " + status.getModificationTime());
                // 显示路径
                System.out.println("Path: " + status.getPath());
            } else {
                System.out.println("远程文件不存在，请检查路径");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            HdfsApi.closeFS(fs);
        }
    }

    /**
     * @Param: [remotePath]
     * @return: void
     * @Author: liyangyang
     * @Date: 2023/6/3 15:32
     * @Description: 给定HDFS中某一个目录，输出该目录下的所有文件的读写权限、大小、创建时间、路径等信息，如果该文件是目录，则递归输出该目录下所有文件相关信息
     */
    public void listFiles(String remotePath) {
//        获取文件对象
        FileSystem fs = HdfsApi.getFS();
//        路径非空判断
        if (remotePath == null) {
            System.out.println("文件输入路径为空，请重试");
            return;
        }
        Path path = new Path(remotePath);
        try {
            //  判断远程文件是否为空
            if (fs.exists(path)) {
                FileStatus[] statuses = fs.listStatus(path);
                for (FileStatus status : statuses) {
                    // 显示读写权限、大小、创建时间、路径等信息
                    System.out.println("Permission: " + status.getPermission());
                    System.out.println("Size: " + status.getLen());
                    System.out.println("Creation time: " + status.getModificationTime());
                    System.out.println("Path: " + status.getPath());
                    // 如果是目录，则递归调用
                    if (status.isDirectory()) listFiles(status.getPath().toString());
                }
            } else {
                System.out.println("远程文件不存在，请检查路径");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            HdfsApi.closeFS(fs);
        }

    }

    /**
     * @Param: [remotePath]
     * @return: void
     * @Author: liyangyang
     * @Date: 2023/6/3 16:16
     * @Description: 提供一个HDFS内的文件的路径，对该文件进行创建和删除操作。如果文件所在目录不存在，则自动创建目录。
     */
    public void createAndDelete(String remotePath) throws IOException {
        FileSystem fs = HdfsApi.getFS();

        if (remotePath == null) {
            System.out.println("文件输入路径为空，请重试");
            return;
        }
        Path path = new Path(remotePath);
        FSDataOutputStream out = null;
        try {
            if (fs.exists(path)) {
                //  存在删除
                fs.delete(path, true);
                Display.delete_file_success(remotePath);
            } else {
                // 创建文件，如果文件所在目录不存在，则自动创建目录
                out = fs.create(path, true);
                out.writeUTF("Hello, HDFS!");
                out.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            HdfsApi.closeFS(fs);
        }
    }


    /**
     * @Param: [remotePath]
     * @return: void
     * @Author: liyangyang
     * @Date: 2023/6/3 17:18
     * @Description: 提供一个HDFS的目录的路径，对该目录进行创建和删除操作。
     * 创建目录时，如果目录文件所在目录不存在则自动创建相应目录。删除目录时，由用户指定当该目录不为空时是否还删除该目录。
     */
    public void createAndDeleteDirByUserOperator(String remotePath) {
        FileSystem fs = HdfsApi.getFS();

        Path dirPath = new Path(remotePath);
        try {
            // 创建目录，如果目录文件所在目录不存在则自动创建相应目录
            if (fs.exists(dirPath) && fs.listStatus(dirPath).length != 0) {
                System.out.println("当前目录下有若干文件，是否要继续删除? (输入 n 或 任意键--不删除，y --删除)");
                // 获取用户输入的文件路径和HDFS文件路径
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                String s = reader.readLine();
                if ("Y".equalsIgnoreCase(s)) {
                    // 删除目录，由用户指定当该目录不为空时是否还删除该目录
                    boolean recursive = true; // 或者 false
                    fs.delete(dirPath, recursive);
                    Display.delete_file_success(remotePath);
                } else {
                    System.out.println("程序结束");
                }
            } else {
                //  目录不存在，则创建目录
                fs.mkdirs(dirPath);
                Display.create_dir_success(remotePath);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /**
    * @Param: [remotePath]
    * @return: void
    * @Author: liyangyang
    * @Date: 2023/6/3 20:28
    * @Description: 向HDFS中指定的文件追加内容，由用户指定内容追加到原有文件的开头或结尾。
    */
    public void appendFileByUserOperator(String remotePath) throws IOException {
//        获取文件对象
        FileSystem fs = HdfsApi.getFS();
//        路径非空判断
        if (!HdfsApi.remotePath_checkNull(remotePath)){
            Display.remotePath_isNull();
            return;
        }else {
            //        获取路径
            Path path = new Path(remotePath);
            //  远程文件非空判断
            if (HdfsApi.remoteFile_isExist(fs,path)){
                //  获取用户输入信息
                String s = GetUserOperator.append();
                //  读取文件内容
                FSDataInputStream fsdInput = null;
                //  输出文件内容
                FSDataOutputStream fsdOutput = null;
                // 如果用户指定内容追加到原有文件的开头
                if ("H".equalsIgnoreCase(s)){
                    try {
                        // 获取文件输入流
                        fsdInput = fs.open(path);

                        // 创建文件
                        fsdOutput = fs.create(path,true);
                        // 追加内容
                        System.out.println("请输入要添加内容:");
                        fsdOutput.write(new BufferedReader(new InputStreamReader(System.in,"UTF-8")).readLine().getBytes());

                        //  添加换行
                        fsdOutput.write("\n".getBytes());
                        // 读取文件原有内容
                        byte[] buffer = new byte[1024];
                        int bytesRead = 0;
                        while ((bytesRead = fsdInput.read(buffer)) > 0) {
                            fsdOutput.write(buffer, 0, bytesRead);
                        }
                        // 关闭文件输出流和文件输入流
                        fsdOutput.close();
                        fsdInput.close();

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        // 关闭文件系统对象
                        HdfsApi.closeFS(fs);
                    }
//                    fsdOutput = fs.create(path, true);

                // 如果用户指定内容追加到原有文件的结尾
                } else if ("T".equalsIgnoreCase(s)) {
                    // 直接写入新内容
                    try {
                        fsdOutput = fs.append(path);
                        System.out.println("请输入要添加内容:");
//                        fsdOutput.write("我的世界".getBytes());
//                        fsdOutput.writeUTF("\n"+new BufferedReader(new InputStreamReader(System.in,"UTF-8")).readLine());
                        fsdOutput.write(new BufferedReader(new InputStreamReader(System.in,"UTF-8")).readLine().getBytes());
                        //  添加换行符
                        fsdOutput.write("\n".getBytes());
                        fsdOutput.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        // 关闭文件系统对象
                        HdfsApi.closeFS(fs);
                    }
                }else {
                    Display.process_exit();
                }
            }else {
                Display.remotePath_noExist();
                HdfsApi.closeFS(fs);
            }
        }
    }



}
