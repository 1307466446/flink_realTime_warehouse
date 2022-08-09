package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Blue红红
 * @description 工具域优惠券领取
 * @create 2022/6/21 10:15
 */
public class DwdToolCouponGet {
    public static void main(String[] args) {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                3, Time.days(1), Time.minutes(1)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(
//                "hdfs://hadoop102:8020/gmall-flink/ck"
//        );
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3.
        tableEnv.executeSql(MyKafkaUtil.getTopic_Db_DDL("dwd_tool_coupon_get"));

        // 
        Table resultTable = tableEnv.sqlQuery("select " +
                "data['id'],  " +
                "data['coupon_id'],  " +
                "data['user_id'],  " +
                "date_format(data['get_time'],'yyyy-MM-dd') date_id,  " +
                "data['get_time']  " +
                "from topic_db " +
                "where `database`='gmall_1227_flink' and `table`='coupon_use' and `type`='insert' ");

        tableEnv.createTemporaryView("result_table ", resultTable);
        
        // 
        tableEnv.executeSql("create table dwd_tool_coupon_get (" +
                "id string,  " +
                "coupon_id string,  " +
                "user_id string,  " +
                "date_id string,  " +
                "get_time string  " +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_tool_coupon_get"));

        // TODO 6. 将数据写入 Kafka-Connector 表
        tableEnv.executeSql("insert into dwd_tool_coupon_get select * from result_table");


    }
}
