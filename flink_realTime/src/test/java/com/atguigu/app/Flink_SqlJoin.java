package com.atguigu.app;

import com.atguigu.bean.Bean1;
import com.atguigu.bean.Bean2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author Blue红红
 * @description 功能描述
 * @create 2022/6/17 23:48
 */
public class Flink_SqlJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        System.out.println(tableEnv.getConfig().getIdleStateRetention());  // 默认是 PT0S 状态不清除
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10)); // 设置状态过期时间为10s

        SingleOutputStreamOperator<Bean1> bean1DS = env.socketTextStream("localhost", 8888)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean1(fields[0], fields[1], Long.parseLong(fields[2]));
                });

        SingleOutputStreamOperator<Bean2> bean2DS = env.socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean2(fields[0], fields[1], Long.parseLong(fields[2]));
                });

        //将两个流转换为两张动态表
        tableEnv.createTemporaryView("t1", bean1DS);
        tableEnv.createTemporaryView("t2", bean2DS);

        //内连接     左边：OnCreateAndWrite    右边：OnCreateAndWrite
//        tableEnv.sqlQuery("select t1.id,t1.name,t2.sex from t1 join t2 on t1.id=t2.id")
//                .execute()
//                .print();

        //左外连接   左边：OnReadAndWrite      右边：OnCreateAndWrite
//        tableEnv.sqlQuery("select t1.id,t1.name,t2.sex from t1 left join t2 on t1.id=t2.id")
//                .execute()
//                .print();

        //右外连接   左边：OnCreateAndWrite    右边：OnReadAndWrite
//        tableEnv.sqlQuery("select t2.id,t1.name,t2.sex from t1 right join t2 on t1.id=t2.id")
//                .execute()
//                .print();

        //全外连接 左边：OnReadAndWrite      右边：OnReadAndWrite
        tableEnv.sqlQuery("select t1.id,t2.id,t1.name,t2.sex from t1 full join t2 on t1.id=t2.id")
                .execute()
                .print();
    }
}
