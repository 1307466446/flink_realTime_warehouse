package com.atguigu.gmall.realtime.app.dwd.db;


import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.atguigu.gmall.realtime.utils.MysqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author Blue红红
 * @description 功能描述
 * @create 2022/6/20 10:53
 */
public class DwdTradeOrderPreProcess {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // kafka对应的主题分区数就是1，所以设置为1
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // join的时候，默认情况下两边数据都是永久保存状态中的
        // 我们不希望状态永久保存，能关联上就可以了，下单这个操作数据会同时写入相关的表中，所以我们设置一个网络最大延迟时间
//        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

//        System.setProperty("HADOOP_USER_NAME", "atguigu");
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall-flink/ck");
//        env.enableCheckpointing(3 * 60000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(3 * 30000L);

        // 获取配置对象
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        // 为表关联时状态中存储的数据设置过期时间
        configuration.setString("table.exec.state.ttl", "5 s");

        // TODO 1.获取topic_db表数据
        tableEnv.executeSql(MyKafkaUtil.getTopic_Db_DDL("order_pre_process_1227"));

        // TODO 2.订单表
        Table orderInfoTable = tableEnv.sqlQuery("select " +
                " data['id'] id, " +
                " data['user_id'] user_id, " +
                " data['province_id'] province_id, " +
                " data['operate_time'] operate_time, " +
                " data['order_status'] order_status, " +
                " `type`, " +
                " `old`, " +
                " `pt` " +
                "from topic_db " +
                "where `database`='gmall_1227_flink' and `table`='order_info' " +
                "and (`type`='insert' or `type`='update') ");
        tableEnv.createTemporaryView("order_info", orderInfoTable);

        // TODO 3.订单明细表
        Table orderDeatilTable = tableEnv.sqlQuery("select " +
                " `data`['id'] id, " +
                " `data`['order_id'] order_id, " +
                " `data`['sku_id'] sku_id, " +
                " `data`['sku_name'] sku_name, " +
                " `data`['create_time'] create_time, " +
                " `data`['source_id'] source_id, " +
                " `data`['source_type'] source_type, " +
                " `data`['sku_num'] sku_num, " +
                "  cast(cast(data['sku_num'] as decimal(16,2)) * cast(data['order_price'] as decimal(16,2)) as String) split_original_amount, " +
                " `data`['split_total_amount'] split_total_amount, " +
                " `data`['split_activity_amount'] split_activity_amount, " +
                " `data`['split_coupon_amount'] split_coupon_amount " +
                "from topic_db " +
                "where `database`='gmall_1227_flink' and `table`='order_detail' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("order_detail", orderDeatilTable);

        // TODO 4.订单明细优惠券表
        Table orderDetailCoupon = tableEnv.sqlQuery("select " +
                " data['order_detail_id'] order_detail_id, " +
                " data['coupon_id'] coupon_id " +
                "from `topic_db` " +
                "where `database`='gmall_1227_flink' and `table` = 'order_detail_coupon' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);


        // TODO 5.订单明细活动表
        Table orderDetailActivity = tableEnv.sqlQuery("select  " +
                " data['order_detail_id'] order_detail_id, " +
                " data['activity_id'] activity_id, " +
                " data['activity_rule_id'] activity_rule_id " +
                "from `topic_db` " +
                "where `database`='gmall_1227_flink' and `table` = 'order_detail_activity' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        // TODO 6.读取mysql的base_dic表 lookUp
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 7.关联5表
        Table resultTable = tableEnv.sqlQuery("select " +
                " od.id, " +
                " od.order_id, " +
                " oi.user_id, " +
                " oi.order_status, " +
                " od.sku_id, " +
                " od.sku_name, " +
                " oi.province_id, " +
                " act.activity_id, " +
                " act.activity_rule_id, " +
                " cou.coupon_id, " +
                " date_format(od.create_time, 'yyyy-MM-dd') date_id, " +
                " od.create_time, " +
                " date_format(oi.operate_time, 'yyyy-MM-dd') operate_date_id, " +
                " oi.operate_time, " +
                " od.source_id, " +
                " od.source_type, " +
                " dic.dic_name source_type_name, " +
                " od.sku_num, " +
                " od.split_original_amount, " +
                " od.split_activity_amount, " +
                " od.split_coupon_amount, " +
                " od.split_total_amount, " +
                " oi.`type`, " +
                " oi.`old`, " +
                " current_row_timestamp() row_op_ts " +  // 撤回流做去重使用
                "from order_info oi " +
                "join order_detail od on od.order_id=oi.id " +
                "left join order_detail_activity act on act.order_detail_id = od.id " +  
                "left join order_detail_coupon cou on cou.order_detail_id = od.id " +
                "join base_dic for system_time as of oi.pt as dic " +
                "on dic.dic_code=od.source_type");
        tableEnv.createTemporaryView("result_table", resultTable);
        


        // TODO 8.写入kafka 撤回流，主键不要忘了
        tableEnv.executeSql("" +
                "create table dwd_trade_order_pre_process( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "order_status string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "date_id string, " +
                "create_time string, " +
                "operate_date_id string, " +
                "operate_time string, " +
                "source_id string, " +
                "source_type string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string, " +
                "`type` string, " +
                "`old` map<string,string>, " +
                "row_op_ts timestamp_ltz(3), " +
                "primary key(id) not enforced " +  // 主键!!!
                ") " + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_order_pre_process"));
        
        tableEnv.executeSql("insert into dwd_trade_order_pre_process select * from result_table").print();


    }
}
