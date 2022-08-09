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

import java.time.ZoneId;

/**
 * @author Blue红红
 * @description 交易域订单事务事实表
 * @create 2022/6/20 19:58
 * 
 * 数据流：web/app -> Nginx -> 业务服务器 -> Mysql(Binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
 * 程  序：  Mock -> Mysql(Binlog) -> Maxwell -> Kafka(ZK) -> DwdTradeOrderPreProcess -> Kafka(ZK) -> DwdTradeOrderDetail -> Kafka(ZK)
 */
public class DwdTradeOrderDetail {
    public static void main(String[] args) {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 启用状态后端
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

        // TODO 3. 读取 Kafka dwd_trade_order_pre_process 主题数据
        tableEnv.executeSql("" +
                "create table dwd_trade_order_pre_process(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "order_status string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "operate_date_id string,\n" +
                "operate_time string,\n" +
                "source_id string,\n" +
                "source_type string,\n" +
                "source_type_name string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string,\n" +
                "`type` string,\n" +
                "`old` map<string,string>,\n" +
                "row_op_ts timestamp_ltz(3)\n" +
                ")" + MyKafkaUtil.getKafkaDDL(
                "dwd_trade_order_pre_process", "dwd_trade_order_detail"));

        // TODO 4. 过滤下单数据
        Table filteredTable = tableEnv.sqlQuery("" +
                "select " +
                "id,\n" +
                "order_id,\n" +
                "user_id,\n" +
                "sku_id,\n" +
                "sku_name,\n" +
                "province_id,\n" +
                "activity_id,\n" +
                "activity_rule_id,\n" +
                "coupon_id,\n" +
                "date_id,\n" +
                "create_time,\n" +
                "source_id,\n" +
                "source_type source_type_code,\n" +
                "source_type_name,\n" +
                "sku_num,\n" +
                "split_original_amount,\n" +
                "split_activity_amount,\n" +
                "split_coupon_amount,\n" +
                "split_total_amount,\n" +
                "row_op_ts\n" +
                "from dwd_trade_order_pre_process " +
                "where `type`='insert'");
        tableEnv.createTemporaryView("filtered_table", filteredTable);

        // TODO 5. 创建 Kafka-Connector 下单明细表
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "source_id string,\n" +
                "source_type_code string,\n" +
                "source_type_name string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string,\n" +
                "row_op_ts timestamp_ltz(3)\n" +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_order_detail"));

        // TODO 6. 将数据写出到 Kafka
        tableEnv.executeSql("insert into dwd_trade_order_detail select * from filtered_table");

    }
}
