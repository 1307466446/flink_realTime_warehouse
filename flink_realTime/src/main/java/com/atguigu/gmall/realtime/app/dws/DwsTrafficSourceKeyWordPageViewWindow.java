package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.func.SplitFunction;
import com.atguigu.gmall.realtime.bean.KeywordBean;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Blue红红
 * @description dws层
 * 来源 关键词粒度 页面浏览 各窗口汇总表
 * @create 2022/6/21 19:11
 */
public class DwsTrafficSourceKeyWordPageViewWindow {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //状态后端设置
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
//                "hdfs://hadoop102:8020/ck"
//        );
//        System.setProperty("HADOOP_USER_NAME", "atguigu");


        // from_unixtime(numeric[, string]) 将数值类型转换为"yyyy-MM-dd HH:mm:ss"的形式字符串
        // to_timestamp(string1[, string2]) 将"yyyy-MM-dd HH:mm:ss"的形式字符串转换为时间戳
        String topic = "dwd_traffic_page_log";
        String groupId = "keyword_page_view_window_1227";
        // TODO 2.创建page_log动态表，并提取watermark
        tableEnv.executeSql("" +
                "create table page_log( " +
                " `page` Map<String,String>, " +
                " `ts` BigInt, " +
                " `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                " watermark for rt as rt- interval '2' second " +
                ") " + MyKafkaUtil.getKafkaDDL(topic, groupId));

        // TODO 3.过滤出keyword的日志
        Table filterTable = tableEnv.sqlQuery("" +
                "select " +
                " page['item'] keyword, " +
                " rt " +   // 开窗的时候需要这个字段
                "from page_log " +
                "where page['last_page_id']='search' " +
                "and page['item_type']='keyword' " +
                "and page['item'] is not null ");
        tableEnv.createTemporaryView("filter_table", filterTable);

        // TODO 4.注册函数
        tableEnv.createTemporaryFunction("SplitFunction", SplitFunction.class);

        // TODO 5.使用UDTF对keyword进行分词
        Table splitTable = tableEnv.sqlQuery("" +
                "select " +
                " word, " +
                " rt " +
                "from filter_table, lateral table(SplitFunction(keyword))");
        tableEnv.createTemporaryView("split_table", splitTable);

        // TODO 6.对分词后的word进行，分组，开窗，聚合

        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                " 'search' source," +
                " date_format(tumble_start(rt,interval '10' second),'yyyy-MM-dd HH:mm:ss') stt, " +
                " date_format(tumble_end(rt,interval '10' second),'yyyy-MM-dd HH:mm:ss') edt, " +
                " word keyword, " +
                " count(*) keyword_count, " +   
                " unix_timestamp() ts " +    // 作为replacingMergeTree的时间字段,用来选择最新的版本
                "from split_table " +
                "group by word, tumble(rt,interval '10' second)");

        // TODO 7.将动态表转换为流
        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toAppendStream(resultTable, KeywordBean.class);

        // TODO 8.将数据写到ClickHouse
        keywordBeanDataStream.addSink(MyClickHouseUtil.getSink("insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)"));
        keywordBeanDataStream.print(">>>>>>>>>");
        
        env.execute("DwsTrafficSourceKeywordPageViewWindow");


    }
}
