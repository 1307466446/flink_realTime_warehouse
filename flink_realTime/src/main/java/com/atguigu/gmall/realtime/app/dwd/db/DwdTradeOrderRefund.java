package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.atguigu.gmall.realtime.utils.MysqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Blue红红
 * @description 交易域退单事实表
 * @create 2022/6/20 23:15
 */
public class DwdTradeOrderRefund {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));
//
//        // 获取配置对象
        Configuration configuration = tableEnv.getConfig().getConfiguration();
//        // 为表关联时状态中存储的数据设置过期时间
        configuration.setString("table.exec.state.ttl", "5 s");
//
//        // TODO 状态后端设置
//        System.setProperty("HADOOP_USER_NAME", "atguigu");
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
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
        // TODO 1.
        tableEnv.executeSql(MyKafkaUtil.getTopic_Db_DDL("dwd_trade_order_refund"));

        // TODO 2.退单表数据，筛选出退单的
        Table orderRefundInfo = tableEnv.sqlQuery("" +
                "select " +
                "data['id'] id,  " +
                "data['user_id'] user_id,  " +
                "data['order_id'] order_id,  " +
                "data['sku_id'] sku_id,  " +
                "data['refund_type'] refund_type,  " +
                "data['refund_num'] refund_num,  " +
                "data['refund_amount'] refund_amount,  " +
                "data['refund_reason_type'] refund_reason_type,  " +
                "data['refund_reason_txt'] refund_reason_txt,  " +
                "data['create_time'] create_time,  " +
                "pt  " +
                "from topic_db " +
                "where `database`='gmall_1227_flink' and `table`='order_refund_info' and `type`='insert' ");

        tableEnv.createTemporaryView("order_refund_info ", orderRefundInfo);

        // TODO 3. 读订单表数据,order_status='1005'
        Table orderInfoRefund = tableEnv.sqlQuery("" +
                "select " +
                "data['id'] id,  " +
                "data['province_id'] province_id,  " +
                "`old`  " +
                "from topic_db " +
                "where `database`='gmall_1227_flink' and `table`='order_info' " +
                "and `type`='update' " +
                "and `old`['order_status'] is not null " +
                "and `data`['order_status']='1005' ");
        tableEnv.createTemporaryView("order_info", orderInfoRefund);
        
        // TODO 4. mysql-lookUp表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());
        
        // TODO 5.关联3表
        Table resultTable = tableEnv.sqlQuery("select   " +
                "ri.id,  " +
                "ri.user_id,  " +
                "ri.order_id,  " +
                "ri.sku_id,  " +
                "oi.province_id,  " +
                "date_format(ri.create_time,'yyyy-MM-dd') date_id,  " +
                "ri.create_time,  " +
                "ri.refund_type,  " +
                "type_dic.dic_name,  " +
                "ri.refund_reason_type,  " +
                "reason_dic.dic_name,  " +
                "ri.refund_reason_txt,  " +
                "ri.refund_num,  " +
                "ri.refund_amount,  " +
                "current_row_timestamp() row_op_ts  " +
                "from order_refund_info ri  " +
                "join   " +
                "order_info oi  " +
                "on ri.order_id = oi.id  " +
                "join   " +
                "base_dic for system_time as of ri.pt as type_dic  " +
                "on ri.refund_type = type_dic.dic_code  " +
                "join  " +
                "base_dic for system_time as of ri.pt as reason_dic  " +
                "on ri.refund_reason_type=reason_dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 6.建表，将结果写出到kafka
        tableEnv.executeSql("create table dwd_trade_order_refund(  " +
                "id string,  " +
                "user_id string,  " +
                "order_id string,  " +
                "sku_id string,  " +
                "province_id string,  " +
                "date_id string,  " +
                "create_time string,  " +
                "refund_type_code string,  " +
                "refund_type_name string,  " +
                "refund_reason_type_code string,  " +
                "refund_reason_type_name string,  " +
                "refund_reason_txt string,  " +
                "refund_num string,  " +
                "refund_amount string,  " +
                "row_op_ts timestamp_ltz(3)  " +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_order_refund"));

        // TODO 9. 将关联结果写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_trade_order_refund select * from result_table");


    }
}
