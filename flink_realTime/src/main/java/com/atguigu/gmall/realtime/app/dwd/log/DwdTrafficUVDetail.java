package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Blue红红
 * @description dwd层uv的一个明细数据
 * 这里我们并没有用start的数据来做uv，而是用page来做的
 *
 * 数据流：web/app -> Nginx -> 日志服务器(log) -> Flume -> Kafka(ODS) ->FlinkApp ->   kafka(dwd)   -> flinkApp         -> kafka(dwd:dwd_traffic_unique_visitor_detail)
 * 程 序：                         lg.sh     -> f1.sh -> zk.sh,kf.sh-> BaseLogApp-> zk.sh,kf.sh ->DwdTrafficUVDetail ->  zk.sh,kf.sh  
 * @create 2022/6/16 13:24
 */
public class DwdTrafficUVDetail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境设置为kafka的分区数
        
/*      设置状态后端
        System.setProperty("HADOOP_USER_NAME","atguigu");
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall-flink/ck");

        env.enableCheckpointing(3 * 60000L);
        env.getCheckpointConfig().setCheckpointTimeout(5*60000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        */

        String topic = "dwd_traffic_page_log";
        String groupId = "uv_1227";
        DataStreamSource<String> pageDStream = env.addSource(MyKafkaUtil.getFlinkConsumer(topic, groupId));

        // TODO 1.过滤掉上一个页面不为null的
        // 只有上个页面为null，证明是这个用户的第一条数据
        SingleOutputStreamOperator<JSONObject> jsonPageDS = pageDStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                if (jsonObject.getJSONObject("page").getString("last_page_id") == null) {
                    out.collect(jsonObject);
                }
            }
        });

        // TODO 2.只保留访客当天的第一条数据
        KeyedStream<JSONObject, String> keyedStream = jsonPageDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> uvDetailDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> last_visit_date_state;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last_visit_date_state", String.class);
                last_visit_date_state = getRuntimeContext().getState(stateDescriptor);
                // 我们给状态设置一个自动重置 例 mid=1 last_visit_date_state中存储的时间为2020-06-10 10:30:10，这个状态会在2020-06-11 10:30:10，过期
                // 如果这期间状态没有被改变，也就是11号mid=1这个访客没在使用该软件，24后状态变为null。
                // 假设这个期间我们的转状态发生了改变2020-06-11 08:10:10 用户使用了该软件，那么状态的过期时间将自动重置到2020-06-12 08:10:10
                // 简单的说：状态发生改变时，也会自动重置过期时间
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)  // 更新状态的时候修改过期时间，往后加24小时
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                String last_visit_date = last_visit_date_state.value();
                String current_date = DateFormatUtil.toDate(value.getLong("ts"));
                if (last_visit_date == null || !last_visit_date.equals(current_date)) {
                    last_visit_date_state.update(current_date);
                    return true;
                }
                return false;
            }
        });
        
    

        uvDetailDS.print("uvDetail>>>>>>");
        String topic1 = "dwd_traffic_unique_visitor_detail";
        uvDetailDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getFlinkProducer(topic1));
        
        env.execute("DwdTrafficUVDetail");


    }
}
