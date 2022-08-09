package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.atguigu.gmall.realtime.utils.MysqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Blue红红
 * @description 功能描述
 * @create 2022/6/21 10:40
 */
public class DwdInteractionComment {

    public static void main(String[] args) {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        // 维表关联时状态中存储的数据设置过期时间
        configuration.setString("table.exec.state.ttl", "5 s");

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
        tableEnv.executeSql(MyKafkaUtil.getTopic_Db_DDL("dwd_interaction_favor_add"));

        Table commentInfo = tableEnv.sqlQuery("select  " +
                "data['id'] id,  " +
                "data['user_id'] user_id,  " +
                "data['sku_id'] sku_id,  " +
                "data['order_id'] order_id,  " +
                "data['create_time'] create_time,  " +
                "data['appraise'] appraise,  " +
                "pt  " +
                "from topic_db  " +
                "where `table` = 'comment_info'  " +
                "and `type` = 'insert'  ");
        tableEnv.createTemporaryView("comment_info", commentInfo);

        // TODO 5. 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 6. 关联两张表
        Table resultTable = tableEnv.sqlQuery("select  " +
                "ci.id,  " +
                "ci.user_id,  " +
                "ci.sku_id,  " +
                "ci.order_id,  " +
                "date_format(ci.create_time,'yyyy-MM-dd') date_id,  " +
                "ci.create_time,  " +
                "ci.appraise,  " +
                "dic.dic_name  " +
                "from comment_info ci  " +
                "join  " +
                "base_dic for system_time as of ci.pt as dic  " +
                "on ci.appraise = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 7. 建立 Kafka-Connector dwd_interaction_comment 表
        tableEnv.executeSql("create table dwd_interaction_comment(  " +
                "id string,  " +
                "user_id string,  " +
                "sku_id string,  " +
                "order_id string,  " +
                "date_id string,  " +
                "create_time string,  " +
                "appraise_code string,  " +
                "appraise_name string  " +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_interaction_comment"));

        // TODO 8. 将关联结果写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_interaction_comment select * from result_table");
    }
}
