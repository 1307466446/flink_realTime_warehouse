package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author Blue红红
 * @description
 * @create 2022/6/17 14:38
 *
 * 数据流：web/app -> Nginx -> 日志服务器(log) -> Flume -> Kafka(ODS) ->FlinkApp ->   kafka(dwd:log)   -> flinkApp         -> kafka(dwd:dwd_traffic_user_jump_detail)
 * 程 序：                         lg.sh     -> f1.sh -> zk.sh,kf.sh-> BaseLogApp-> zk.sh,kf.sh -> DwdTrafficJumpDetail  ->  zk.sh,kf.sh  
 */
public class DwdTrafficJumpDetail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall-flink/ck");
//
//        env.enableCheckpointing(3 * 60000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(5 * 60000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);

        String topic = "dwd_traffic_page_log";
        String groupId = "user_jump_detail_1227";
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtil.getFlinkConsumer(topic, groupId));

        // TODO 2.将数据转换为JSON格式，并指定事件时间
        SingleOutputStreamOperator<JSONObject> jsonWithWMDS = dataStreamSource.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        }));
        // TODO 3.分组
        KeyedStream<JSONObject, String> keyedStream = jsonWithWMDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        // TODO 4.定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value, Context<JSONObject> ctx) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                .times(2)
                .consecutive()
                .within(Time.seconds(10));

        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // TODO 5提取匹配上的事件，以及超时的事件(放在测输出流)
        OutputTag<String> timeOutJumpData = new OutputTag<String>("timeOutJumpData") {
        };
        SingleOutputStreamOperator<String> selectDS = patternStream.select(timeOutJumpData, new PatternTimeoutFunction<JSONObject, String>() {
            @Override
            public String timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {

                return pattern.get("start").get(0).toJSONString(); // 超时的第一条数据我们需要的
            }
        }, new PatternSelectFunction<JSONObject, String>() {
            @Override
            public String select(Map<String, List<JSONObject>> pattern) throws Exception {

                return pattern.get("start").get(0).toJSONString();
            }
        });
        DataStream<String> timeOutDS = selectDS.getSideOutput(timeOutJumpData);

        selectDS.print("selectDS>>>>>");
        timeOutDS.print("timeOutDS>>>>>");

        //  TODO 6.合并两条流
        DataStream<String> unionDS = selectDS.union(timeOutDS);


        // TODO 7. 写到kafka
        String topic1 = "dwd_traffic_user_jump_detail";
        unionDS.addSink(MyKafkaUtil.getFlinkProducer(topic1));
        env.execute("dwdTrafficJumpDetail");


    }
}
