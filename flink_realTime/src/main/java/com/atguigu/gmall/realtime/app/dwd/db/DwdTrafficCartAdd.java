package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.atguigu.gmall.realtime.utils.MysqlUtil;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Blue红红
 * @description 交易域加购事务事实表
 * @create 2022/6/19 14:20
 */
public class DwdTrafficCartAdd {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境设置为Kafka的分区数
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall-flink/ck");
//        env.enableCheckpointing(3 * 60000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(5 * 60000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);

        // TODO 1.消费kafka的ODS层topic_db,创建topic_db表并写入数据
        String groupId = "cart_add_1227";
        tableEnv.executeSql(MyKafkaUtil.getTopic_Db_DDL(groupId));

        // TODO 2.从topic_db表中过滤出加购数据
        Table table = tableEnv.sqlQuery("select " +
                "  `data`['id'] id, " +
                "  `data`['user_id'] user_id, " +
                "  `data`['sku_id'] sku_id, " +
                "  if( `type` = 'insert', `data`['sku_num'], cast(cast(`data`['sku_num'] as int)- cast(`old`['sku_num'] as int) as string) ) sku_num, " +
                "  `data`['sku_name'] sku_name, " +
                "  `data`['is_checked'] is_checked, " +
                "  `data`['create_time'] create_time, " +
                "  `data`['operate_time'] operate_time, " +
                "  `data`['is_ordered'] is_ordered, " +
                "  `data`['order_time'] order_time, " +
                "  `data`['source_type'] source_type, " +
                "  `data`['source_id'] source_id," +
                "   pt " +
                "   from topic_db " +
                " where `database` = 'gmall_1227_flink' and `table` = 'cart_info' " +
                " and (`type` = 'insert' or (type = 'update' and `old`['sku_num'] is not null and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int) ) )");
        
        tableEnv.createTemporaryView("cart_info", table);

        // TODO 3.和维表base_dic进行join，做维度退化
        
        
        // 注意我们用LookUpJoin去关联mysql中维表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    ci.id, " +
                "    ci.user_id, " +
                "    ci.sku_id, " +
                "    ci.sku_num, " +
                "    ci.sku_name, " +
                "    ci.is_checked, " +
                "    ci.create_time, " +
                "    ci.operate_time, " +
                "    ci.is_ordered, " +
                "    ci.order_time, " +
                "    ci.source_type, " +
                "    dic.dic_name, " +
                "    ci.source_id " +
                "from cart_info ci " +
                "join base_dic FOR SYSTEM_TIME AS OF ci.pt dic " +
                "on ci.source_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);


        // TODO 4.将查询的数据写入kafka。加购事实表
        tableEnv.executeSql("" +
                "create table dwd_cart_info( " +
                "    `id` String, " +
                "    `user_id` String, " +
                "    `sku_id` String, " +
                "    `sku_num` String, " +
                "    `sku_name` String, " +
                "    `is_checked` String, " +
                "    `create_time` String, " +
                "    `operate_time` String, " +
                "    `is_ordered` String, " +
                "    `order_time` String, " +
                "    `source_type` String, " +
                "    `dic_name` String, " +
                "    `source_id` String " +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_cart_add"));

        //TODO 7.将退化之后的表写出到Kafka
        tableEnv.executeSql("insert into dwd_cart_info select * from result_table");


    }
}
