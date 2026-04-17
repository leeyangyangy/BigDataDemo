package xyz.leeyangy;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@MapperScan("xyz.leeyangy.spc.mapper")
@EnableScheduling
public class ScalaActionApplication {
    public static void main(String[] args) {
        SpringApplication.run(ScalaActionApplication.class, args);
    }
}
