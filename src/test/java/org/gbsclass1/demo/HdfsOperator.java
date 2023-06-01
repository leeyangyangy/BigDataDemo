package org.gbsclass1.demo;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbsclass1.util.HdfsHelper;

//操作hdfs对象
public class HdfsOperator {

//    在hdfs上创建文件夹
    public static void mkdir2HDFS(String remotePath){
//        获取FIleSystem对象
        FileSystem fs = HdfsHelper.getFS();
        if (fs !=null){
           Path path= new Path(remotePath);
//            检查远端目录是否存在
            try {
                boolean exists = fs.exists(path);
                if(!exists){
//                    不存在
                    boolean mkdirs = fs.mkdirs(path);
                    System.out.println("创建结果："+mkdirs);
                }else {
                    System.out.println("创建失败，文件已存在");
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) {
        HdfsOperator.mkdir2HDFS("/bbb");
    }
}
