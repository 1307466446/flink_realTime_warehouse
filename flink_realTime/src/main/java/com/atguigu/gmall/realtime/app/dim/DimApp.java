package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimSinkFunction;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author Blue红红
 * @description 功能描述
 * @create 2022/6/13 23:36
 * 
 * 
 * 数据流：web/app -> Nginx -> 业务服务器 -> Mysql(Binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Phoenix(DIM)
 * 程  序：mock(手动添加) -> Mysql -> Maxwell -> Kafka(ZK) -> DimApp -> Phoenix(HBase/ZK、HDFS)
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境设置为Kafka的分区数
/*        // 1.1 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall-flink/ck");
        // 1.2 开启CK
        env.enableCheckpointing(3 * 60000L); // 3min做一次
        env.getCheckpointConfig().setCheckpointTimeout(5 * 60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); */

        // TODO 2.读取kafka 中topic_db的数据
        String topic = "topic_db";
        String groupId = "dim_app_1227";
        DataStreamSource<String> kafkaDStream = env.addSource(MyKafkaUtil.getFlinkConsumer(topic, groupId));

        // TODO 3.将读取到的数据转换为JSON类型，并过滤非json格式数据
        OutputTag<String> dirtyTag = new OutputTag<String>("dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonDStream = kafkaDStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(new OutputTag<String>("dirty") {
                    }, value);
                }
            }
        });
        jsonDStream.getSideOutput(dirtyTag).print("脏数据>>>>>>>>>>");

        // TODO 4.使用FLinkCDC读取配置表信息
        // flinkCDC做ck，存储的是binlog的位置信息，以状态的形式保存到ck，发生故障时会从cp or sp 中断点续传
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("gmall_1227_config")
                .tableList("gmall_1227_config.table_process")   // 必须是库名+表名!!!!
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> mySqlSourceDStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySqlSource");

        // TODO 5.将配置信息流处理成广播流
            // 广播流中需要使用mapState状态后端
            // key:存表名sourceTable value：存TableProcess
        MapStateDescriptor<String, TableProcess> map_state = new MapStateDescriptor<>("map_state", String.class, TableProcess.class);
            // 将dataDStream转换成广播流
        BroadcastStream<String> broadcastStream = mySqlSourceDStream.broadcast(map_state);

        // TODO 6.连接主流与广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonDStream.connect(broadcastStream);


        // TODO 7.根据广播流发来的tableProcess来处理主流的数据，过滤出属于各个维表的数据
        SingleOutputStreamOperator<JSONObject> hBaseDStream = connectedStream.process(new TableProcessFunction(map_state));
        
        hBaseDStream.print("可以写入到hbase中的数据>>>>>");
    
        // TODO 8.将数据写到hbase的phoenix
        hBaseDStream.addSink(new DimSinkFunction());

        // TODO 9.启动任务
        env.execute("DimAPP");

    }


}
