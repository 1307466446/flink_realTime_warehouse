package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Blue红红
 * @description 用户域用户注册事实表
 * @create 2022/6/21 10:43
 */
public class DwdUserRegister {
    public static void main(String[] args) {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        // 为表关联时状态中存储的数据设置过期时间
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
        tableEnv.executeSql(MyKafkaUtil.getTopic_Db_DDL("dwd_user_register"));
        
        // 
        Table userInfo = tableEnv.sqlQuery("select  " +
                "data['id'] user_id,  " +
                "data['create_time'] create_time  " +
                "from topic_db  " +
                "where `table` = 'user_info'  " +
                "and `type` = 'insert'  ");
        tableEnv.createTemporaryView("user_info", userInfo);

        // 
        tableEnv.executeSql("create table `dwd_user_register`(  " +
                "`user_id` string,  " +
                "`date_id` string,  " +
                "`create_time` string " +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_user_register"));
        

        // TODO 6. 将输入写入 Kafka-Connector 表
        tableEnv.executeSql("insert into dwd_user_register  " +
                "select   " +
                "user_id,  " +
                "date_format(create_time, 'yyyy-MM-dd') date_id,  " +
                "create_time   " +
                "from user_info");

    }
}
