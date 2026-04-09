package xyz.leeyangy.WordCount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ProjectName : SparkLearn
 * @Package : xyz.leeyangy.WordCount
 * @ClassName : WordCount02
 * @Author : leeyangy
 * @CreateTime : 2026/4/8 00:27
 * @Version : 1.0
 * @Description : 
 * @Modify_log : 
 */
object WordCount02 {
  def main(args: Array[String]): Unit = {
    // 1、创建 Spark 运行上下文
    val conf = new SparkConf().setAppName("WordCount_Reduce").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 2、读取 textFile 获取文件
    // 读取单个或多个文件
    val linesRdd = sc.textFile("E:\\BigData\\Spark\\SparkPractice\\SparkBasis\\Datas\\WordCount\\*")

    // 3、扁平化操作
    val wordsRdd = linesRdd.flatMap(line => line.split("\\s+"))

    // 4、结构转换(单词, 1)
    val pairRdd = wordsRdd.map(word => (word, 1))

    // 5、利用 reduceByKey 完成聚合
    val wordCountsRdd = pairRdd.reduceByKey((x, y) => x + y)

    wordCountsRdd.collect().foreach(println)

    sc.stop()
  }
}

