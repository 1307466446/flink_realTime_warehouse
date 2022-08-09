package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author Blue红红
 * @description dwd层
 * 流量域未经加工的事务事实表
 * 将ods层中topic_log数据按照 启动，页面，曝光，动作，错误 进行分流,写到不同的topic中
 * @create 2022/6/15 14:22
 * <p>
 * 数据流：web/app -> Nginx -> 日志服务器(log) -> Flume -> Kafka(ODS) ->FlinkApp -> kafka(dwd)
 * 程 序：                         lg.sh     -> f1.sh -> zk.sh,kf.sh-> BaseLogApp-> zk.sh,kf.sh
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境设置为Kafka的分区数

        /*      
        设置状态后端
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall-flink/ck");
        开启CK，并设置相关参数
        env.enableCheckpointing(3 * 60000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(5 * 60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        */

        String topic = "topic_log";
        String groupId = "base_log_app_1227";
        DataStreamSource<String> logDStream = env.addSource(MyKafkaUtil.getFlinkConsumer(topic, groupId));

        // TODO 1.过滤并转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonLogDStream = logDStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("数据为非法格式。。。。" + value);
                }
            }
        });
        // TODO 2.is_New字段的修复问题
        // 情况1.前面传来的is_new=1，且状态中保存的数据为null，则今天是新用户，将日期写入状态
        //      前面传来的is_new=1，但状态中有日期的，判断日期是否为今天，不是则将is_new改为0
        // 情况3.前面传来的is_new=0，但状态中日期为null，则将前一天日期写入状态
        KeyedStream<JSONObject, String> keyedStream = jsonLogDStream.keyBy(json -> json.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> dataStream = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitorDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitorDateState = getRuntimeContext().getState(new ValueStateDescriptor<>("last_visitDate", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                String lastVisitorDate = lastVisitorDateState.value();
                String is_new = value.getJSONObject("common").getString("is_new");
                Long ts = value.getLong("ts");
                String currentDate = DateFormatUtil.toDate(ts);
                if ("1".equals(is_new)) {
                    if (lastVisitorDate == null) {
                        lastVisitorDateState.update(currentDate);
                    } else if (!currentDate.equals(lastVisitorDate)) {
                        value.getJSONObject("common").put("is_new", 0);
                    }
                } else {
                    if (lastVisitorDate == null) {
                        lastVisitorDateState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000));
                    }
                }
                return value;
            }
        });

        // TODO 3.使用测输出流进行分流处理
        // page在主流，其他都在侧输出流
        OutputTag<String> startOutPutTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayOutPutTag = new OutputTag<String>("display") {
        };
        OutputTag<String> actionOutPutTag = new OutputTag<String>("action") {
        };
        OutputTag<String> errorOutPutTag = new OutputTag<String>("error") {
        };

        SingleOutputStreamOperator<String> pageDStream = dataStream.process(new ProcessFunction<JSONObject, String>() {

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

                // start和page中都有可能出现err，先把err的输出，后面就全是正确的value了，就不用再处理err了。
                String err = value.getString("err");
                if (err != null) {
                    ctx.output(errorOutPutTag, value.toJSONString());
                }

                // 先判断是否为启动
                String start = value.getString("start");
                if (start != null) {
                    value.remove("err");
                    ctx.output(startOutPutTag, value.toJSONString());
                } else {
                    // 我们希望把 page,common,ts也放进display和action中,例如：这个display或者action是在那个页面,那个时间，那个设备 做的
                    JSONObject page = value.getJSONObject("page");
                    JSONObject common = value.getJSONObject("common");
                    Long ts = value.getLong("ts");

                    // displays
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject jsonObject = displays.getJSONObject(i);
                            jsonObject.put("common", common);
                            jsonObject.put("page", page);
                            jsonObject.put("ts", ts);
                            ctx.output(displayOutPutTag, jsonObject.toJSONString());
                        }
                    }

                    //actions
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject jsonObject = actions.getJSONObject(i);
                            jsonObject.put("common", common);
                            jsonObject.put("page", page);
                            jsonObject.put("ts", ts);
                            ctx.output(actionOutPutTag, jsonObject.toJSONString());
                        }
                    }

                    // page
                    // page的数据放入主流
                    value.remove("actions");
                    value.remove("displays");
                    value.remove("err");
                    out.collect(value.toJSONString());

                }
            }
        });

        DataStream<String> startDStream = pageDStream.getSideOutput(startOutPutTag);
        DataStream<String> displayDStream = pageDStream.getSideOutput(displayOutPutTag);
        DataStream<String> actionDStream = pageDStream.getSideOutput(actionOutPutTag);
        DataStream<String> errDStream = pageDStream.getSideOutput(errorOutPutTag);

        startDStream.print("startDStream>>>>>");
        displayDStream.print("displayDStream>>>>>");
        actionDStream.print("actionDStream>>>>>");
        errDStream.print("errDStream>>>>>");

        //TODO 4.将数据写出到对应的Kafka主题
        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        startDStream.addSink(MyKafkaUtil.getFlinkProducer(start_topic));
        displayDStream.addSink(MyKafkaUtil.getFlinkProducer(display_topic));
        actionDStream.addSink(MyKafkaUtil.getFlinkProducer(action_topic));
        errDStream.addSink(MyKafkaUtil.getFlinkProducer(error_topic));
        pageDStream.addSink(MyKafkaUtil.getFlinkProducer(page_topic));

        env.execute("BaseLogApp");


    }
}
