package xyz.leeyangy.WordCount

import scala.io.Source

/**
 * @ProjectName : SparkLearn
 * @Package : xyz.leeyangy.WordCount
 * @ClassName : WordCount01
 * @Author : leeyangy
 * @CreateTime : 2026/4/8 00:25
 * @Version : 1.0
 * @Description : 
 * @Modify_log : 
 */
object WordCount01 {
  def main(args: Array[String]): Unit = {
    // 1、文件路径
    val filePaths = Seq(
      "E:\\BigData\\Spark\\SparkPractice\\SparkBasis\\Datas\\WordCount\\1.txt",
      "E:\\BigData\\Spark\\SparkPractice\\SparkBasis\\Datas\\WordCount\\2.txt"
    )

    // 读取所有文件内容
    val words = filePaths.flatMap(path => Source.fromFile(path).getLines()).flatMap(_.split("\\s+"))

    // 将单词转换成键值对形式
    val wordcounts = words.groupBy(word => word).map(kv => (kv._1, kv._2.size))

    wordcounts.foreach(println)
  }
}
