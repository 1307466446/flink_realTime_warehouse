package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Blue红红
 * @description 交易域领券支付
 * @create 2022/6/21 10:35
 */
public class DwdOrderCouponPay {
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
        tableEnv.executeSql(MyKafkaUtil.getTopic_Db_DDL("dwd_tool_coupon_pay"));

        // 
        Table couponUsePay = tableEnv.sqlQuery("select  " +
                "data['id'] id,  " +
                "data['coupon_id'] coupon_id,  " +
                "data['user_id'] user_id,  " +
                "data['order_id'] order_id,  " +
                "date_format(data['used_time'],'yyyy-MM-dd') date_id,  " +
                "data['used_time'] used_time,  " +
                "`old`  " +
                "from topic_db  " +
                "where `table` = 'coupon_use'  " +
                "and `type` = 'update'  " +
                "and data['used_time'] is not null");

        tableEnv.createTemporaryView("coupon_use_pay", couponUsePay);

        // 
        tableEnv.executeSql("create table dwd_tool_coupon_pay(  " +
                "id string,  " +
                "coupon_id string,  " +
                "user_id string,  " +
                "order_id string,  " +
                "date_id string,  " +
                "payment_time string  " +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_tool_coupon_pay"));

        // TODO 6. 将数据写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_tool_coupon_pay select " +
                "id,  " +
                "coupon_id,  " +
                "user_id,  " +
                "order_id,  " +
                "date_id,  " +
                "used_time payment_time  " +
                "from coupon_use_pay");

    }
}
