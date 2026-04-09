package xyz.leeyangy.WordCount

import org.apache.spark.{SparkConf, SparkContext}
/**
 * @ProjectName : SparkLearn
 * @Package : xyz.leeyangy.WordCount
 * @ClassName : WordCount03
 * @Author : leeyangy
 * @CreateTime : 2026/4/8 00:27
 * @Version : 1.0
 * @Description : 
 * @Modify_log : 
 */
object WordCount03 {
  def main(args: Array[String]): Unit = {
    // 1、创建 Spark 运行上下文
    val conf = new SparkConf().setAppName("WordCount_PatternMatching").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 2、读取 textFile 获取文件
    // 读取单个或多个文件
    val linesRdd = sc.textFile("E:\\BigData\\Spark\\SparkPractice\\SparkBasis\\Datas\\WordCount\\*")

    // 3、扁平化操作
    val wordsRdd = linesRdd.flatMap(line => line.split("\\s+"))

    // 4、结构转换
    val pairRdd = wordsRdd.map(word => (word, 1))

    // 5、利用 groupByKey 对 key 进行分组，再对 value 值进行聚合
    val groupedRdd = pairRdd.groupByKey()

    // 6、(自己选择) 利用 map 将每个元素处理成最终结果
    val wordCountsRdd = groupedRdd.map {
      case (word, ones) => (word, ones.sum)
      // case (word, ones) => (word, ones.size)  // 对于 (word, 1) 的情况, .size 和 .sum 结果一样
    }

    wordCountsRdd.collect().foreach(println)

    sc.stop()
  }
}

