package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.atguigu.gmall.realtime.utils.MysqlUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * @author Blue红红
 * @description 交易域支付成功事务事实表
 * @create 2022/6/20 21:35
 */
public class DwdTradePayDetailSuc {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));
//
//        // 获取配置对象
        Configuration configuration = tableEnv.getConfig().getConfiguration();
//        // 为表关联时状态中存储的数据设置过期时间
        configuration.setString("table.exec.state.ttl", "905 s");
//
//        // TODO 1. 状态后端设置
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

        // TODO 2.从kafka topic：dwd_trade_order_detail 获取数据
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail( " +
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
                "create_time string,  " +
                "source_id string,  " +
                "source_type_code string,  " +
                "source_type_name string,  " +
                "sku_num string,  " +
                "split_original_amount string,  " +
                "split_activity_amount string,  " +
                "split_coupon_amount string,  " +
                "split_total_amount string,  " +
                "row_op_ts timestamp_ltz(3)  " +
                ") " + MyKafkaUtil.getKafkaDDL("dwd_trade_order_detail", "dwd_trade_pay_detail_suc"));

        // TODO 3.获取topic_db
        tableEnv.executeSql(MyKafkaUtil.getTopic_Db_DDL("dwd_trade_pay_detail_suc"));

        // TODO 4.从topic_db中过滤支付成功表的数据
        Table paymentInfo = tableEnv.sqlQuery("select " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "data['payment_type'] payment_type, " +
                "data['callback_time'] callback_time, " +
                "`pt` " +
                "from topic_db " +
                "where `table` = 'payment_info' "
                +
                "and `type` = 'update' " +
                "and data['payment_status']='1602'"
        );
        tableEnv.createTemporaryView("payment_info", paymentInfo);

        // TODO 5.LookUpJoin mysql的base_dic
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 6.关联3表 
        Table result_table = tableEnv.sqlQuery("" +
                "select  " +
                "od.id order_detail_id,  " +
                "od.order_id,  " +
                "od.user_id,  " +
                "od.sku_id,  " +
                "od.sku_name,  " +
                "od.province_id,  " +
                "od.activity_id,  " +
                "od.activity_rule_id,  " +
                "od.coupon_id,  " +
                "pi.payment_type payment_type_code,  " +
                "dic.dic_name payment_type_name,  " +
                "pi.callback_time,  " +
                "od.source_id,  " +
                "od.source_type_code,  " +
                "od.source_type_name,  " +
                "od.sku_num,  " +
                "od.split_original_amount,  " +
                "od.split_activity_amount,  " +
                "od.split_coupon_amount,  " +
                "od.split_total_amount split_payment_amount,  " +
                "od.row_op_ts row_op_ts  " +
                "from payment_info pi  " +
                "join dwd_trade_order_detail od  " +
                "on pi.order_id = od.order_id  " +
                "join `base_dic` for system_time as of pi.pt as dic  " + // 注意时间
                "on pi.payment_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", result_table);
        

        // TODO 7.写入kafka的topic:
        tableEnv.executeSql("" +
                "create table dwd_trade_pay_detail_suc( " +
                "order_detail_id string,  " +
                "order_id string,  " +
                "user_id string,  " +
                "sku_id string,  " +
                "sku_name string,  " +
                "province_id string,  " +
                "activity_id string,  " +
                "activity_rule_id string,  " +
                "coupon_id string,  " +
                "payment_type_code string,  " +
                "payment_type_name string,  " +
                "callback_time string,  " +
                "source_id string,  " +
                "source_type_code string,  " +
                "source_type_name string,  " +
                "sku_num string,  " +
                "split_original_amount string,  " +
                "split_activity_amount string,  " +
                "split_coupon_amount string,  " +
                "split_payment_amount string,  " +
                "row_op_ts timestamp_ltz(3),  " +
                "primary key(order_detail_id) not enforced  " +
                ") " + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_pay_detail_suc"));

        tableEnv.executeSql("insert into dwd_trade_pay_detail_suc select * from result_table");

    }
}
