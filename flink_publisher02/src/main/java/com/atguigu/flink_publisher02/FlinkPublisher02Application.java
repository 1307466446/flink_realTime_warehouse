package com.atguigu.flink_publisher02;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication

@MapperScan(basePackages = "com.atguigu.flink_publisher02.mapper")
public class FlinkPublisher02Application {

    public static void main(String[] args) {
        SpringApplication.run(FlinkPublisher02Application.class, args);
    }

}
