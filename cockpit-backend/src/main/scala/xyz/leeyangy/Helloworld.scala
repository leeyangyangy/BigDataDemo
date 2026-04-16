package xyz.leeyangy
import org.springframework.web.bind.annotation.{GetMapping, RestController}

/**
 * @ProjectName : SparkLearn
 * @Package : xyz.leeyangy
 * @ClassName : Helloworld
 * @Author : leeyangy
 * @CreateTime : 2026/4/8 01:56
 * @Version : 1.0
 * @Description : 
 * @Modify_log : 
 */
//object Helloworld {
//  def main(args: Array[String]): Unit = {
//    println("test Hello World!!!")
//  }
//}

//scalaCopy codepackage xyz.leeyangy
@RestController
class Helloworld {
  @GetMapping(Array("/hello"))
  def hello(): String = {
    "Hello, Scala!"
  }
}