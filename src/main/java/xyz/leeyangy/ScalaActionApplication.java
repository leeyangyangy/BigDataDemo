package xyz.leeyangy;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ProjectName : SparkLearn
 * @Package : xyz.leeyangy
 * @ClassName : ScalaAction
 * @Author : leeyangy
 * @CreateTime : 2026/4/8 02:17
 * @Version : 1.0
 * @Description :
 * @Modify_log :
 */
@SpringBootApplication
@RestController
public class ScalaActionApplication {
    public static void main(String[] args) {
        SpringApplication.run(ScalaActionApplication.class, args);
    }

    @GetMapping("/")
    public String index() {
        return "Hello Scala";
    }
}
