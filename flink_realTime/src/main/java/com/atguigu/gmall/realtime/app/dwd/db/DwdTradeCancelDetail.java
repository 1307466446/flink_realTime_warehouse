package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Blue红红
 * @description 交易域取消订单事务事实表
 * @create 2022/6/20 20:49
 */
public class DwdTradeCancelDetail {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.setRestartStrategy(
//                RestartStrategies.failureRateRestart(3, Time.days(1L), Time.minutes(3L))
//        );
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
//        System.setProperty("HADOOP_USER_NAME", "atguigu");
//
//        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));


        tableEnv.executeSql("" +
                "create table dwd_trade_order_pre_process(  " +
                "id string,  " +
                "order_id string,  " +
                "user_id string,  " +
                "order_status string,  " +
                "sku_id string,  " +
                "sku_name string,  " +
                "province_id string,  " +
                "activity_id string,  " +
                "activity_rule_id string,  " +
                "coupon_id string,  " +
                "date_id string,  " +
                "create_time string,  " +
                "operate_date_id string,  " +
                "operate_time string,  " +
                "source_id string,  " +
                "source_type string,  " +
                "source_type_name string,  " +
                "sku_num string,  " +
                "split_original_amount string,  " +
                "split_activity_amount string,  " +
                "split_coupon_amount string,  " +
                "split_total_amount string,  " +
                "`type` string,  " +
                "`old` map<string,string>,  " +
                "row_op_ts timestamp_ltz(3) " +
                ") " + MyKafkaUtil.getKafkaDDL("dwd_trade_order_pre_process", "dwd_trade_cancel_detail")
        );

        Table filteredTable = tableEnv.sqlQuery("" +
                "select " +
                "id,  " +
                "order_id,  " +
                "user_id,  " +
                "sku_id,  " +
                "sku_name,  " +
                "province_id,  " +
                "activity_id,  " +
                "activity_rule_id,  " +
                "coupon_id,  " +
                "operate_date_id date_id,  " +
                "operate_time cancel_time,  " +
                "source_id,  " +
                "source_type source_type_code,  " +
                "source_type_name,  " +
                "sku_num,  " +
                "split_original_amount,  " +
                "split_activity_amount,  " +
                "split_coupon_amount,  " +
                "split_total_amount,  " +
                "row_op_ts  " +
                "from dwd_trade_order_pre_process " +
                "where `type`='update' and `old`[order_status] is not null and order_status='1003' ");
        tableEnv.createTemporaryView("filteredTable ", filteredTable);

        tableEnv.executeSql("" +
                "create table dwd_trade_cancel_detail(" +
                "id string,  " +
                "order_id string,  " +
                "user_id string,  " +
                "sku_id string,  " +
                "sku_name string,  " +
                "province_id string,  " +
                "activity_id string,  " +
                "activity_rule_id string,  " +
                "coupon_id string,  " +
                "date_id string,  " +
                "cancel_time string,  " +
                "source_id string,  " +
                "source_type_code string,  " +
                "source_type_name string,  " +
                "sku_num string,  " +
                "split_original_amount string,  " +
                "split_activity_amount string,  " +
                "split_coupon_amount string,  " +
                "split_total_amount string,  " +
                "row_op_ts timestamp_ltz(3)  " +
                ") " + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_cancel_detail"));
        
        
        tableEnv.executeSql("insert into dwd_trade_cancel_detail select * from filteredTable");
    }
}
