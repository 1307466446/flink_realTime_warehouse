package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Blue红红
 * @description 工具域领取优惠券下单
 * @create 2022/6/21 10:26
 */
public class DwdToolCouponOrder {
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
        tableEnv.executeSql(MyKafkaUtil.getTopic_Db_DDL("dwd_tool_coupon_order"));

        //
        Table couponUseOrder = tableEnv.sqlQuery("select  " +
                "data['id'] id,  " +
                "data['coupon_id'] coupon_id,  " +
                "data['user_id'] user_id,  " +
                "data['order_id'] order_id,  " +
                "date_format(data['using_time'],'yyyy-MM-dd') date_id,  " +
                "data['using_time'] using_time  " +
                "from topic_db  " +
                "where `table` = 'coupon_use'  " +
                "and `type` = 'update'  " +
                "and data['coupon_status'] = '1402'  " +
                "and `old`['coupon_status'] = '1401'");

        tableEnv.createTemporaryView("result_table", couponUseOrder);

        //
        tableEnv.executeSql("create table dwd_tool_coupon_order(  " +
                "id string,  " +
                "coupon_id string,  " +
                "user_id string,  " +
                "order_id string,  " +
                "date_id string,  " +
                "order_time string  " +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_tool_coupon_order"));

        // 
        tableEnv.executeSql("" +
                "insert into dwd_tool_coupon_order select " +
                "id,  " +
                "coupon_id,  " +
                "user_id,  " +
                "order_id,  " +
                "date_id,  " +
                "using_time order_time  " +
                "from result_table");
    }
}
