package com.atguigu.app;

import com.atguigu.bean.Bean1;
import com.atguigu.bean.Bean2;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Blue红红
 * @description 将撤回流写入kafka，leftJoin时是会有+I 和 -U 操作的
 * @create 2022/6/18 23:50
 */
public class Flink_UpsertSinkKafka {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Bean1> bean1DS = env.socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean1(fields[0], fields[1], Long.parseLong(fields[2]));
                });

        SingleOutputStreamOperator<Bean2> bean2DS = env.socketTextStream("localhost", 8888)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean2(fields[0], fields[1], Long.parseLong(fields[2]));
                });

        tableEnv.createTemporaryView("t1", bean1DS);
        tableEnv.createTemporaryView("t2", bean2DS);

        // 将leftJoin关联上的数据写出
        Table tableResult = tableEnv.sqlQuery("select t1.id,t1.name,t2.sex from t1 left join t2 on t1.id=t2.id");
        tableEnv.createTemporaryView("result_table", tableResult);

        tableEnv.executeSql("" +
                " create table test( " +
                " id String, " +
                " name String, " +
                " sex String," +
                " PRIMARY KEY (id) NOT ENFORCED" + // 主键不能丢，我们撤回流依赖主键
                " ) " + MyKafkaUtil.getUpsertKafkaDDL("test"));

        tableEnv.executeSql("insert into test select * from result_table");

    }
}
