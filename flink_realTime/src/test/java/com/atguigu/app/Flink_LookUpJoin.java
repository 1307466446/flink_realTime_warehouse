package com.atguigu.app;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Blue红红
 * @description 功能描述
 * @create 2022/6/18 10:56
 */
public class Flink_LookUpJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从mysql中读取维表的数据并构建维表
        tableEnv.executeSql(
                "CREATE TEMPORARY TABLE base_dic ( " +
                        " dic_code STRING, " +
                        " dic_name STRING, " +
                        " parent_code STRING, " +
                        " create_time STRING, " +
                        " operate_time STRING " +
                        ") WITH ( " +
                        " 'connector' = 'jdbc'," +
                        " 'url' = 'jdbc:mysql://hadoop102:3306/gmall_1227_flink'," +
                        " 'table-name' = 'base_dic'," +
                        " 'driver' = 'com.mysql.cj.jdbc.Driver'," +
                        " 'lookup.cache.max-rows' = '10'," +  // 设置的从mysql读取来的数据缓存条数
                        " 'lookup.cache.ttl' = '1 hour'," + // 设置缓存时间，缺点是不能实时的对mysql中更改的数据进行获取 
                        " 'username' = 'root'," +
                        " 'password' = 'root'" +
                        ")");

        // 读取kafka中数据到表中
        tableEnv.executeSql("" +
                "CREATE TABLE order_info ( " +
                "  `id` STRING, " +
                "  `state_code` STRING, " +
                "  `pt` AS PROCTIME() " +
                ") " + MyKafkaUtil.getKafkaDDL("test", "test_1227"));

        // 关联主表和维表数据
        tableEnv.executeSql("" +
                " select " +
                "    id, state_code, dic_code, dic_name " +
                " from " +
                "    order_info o " +
                " join " +
                "    base_dic FOR SYSTEM_TIME AS OF pt dic " +
                " on o.state_code=dic.dic_code").print();


    }
}
