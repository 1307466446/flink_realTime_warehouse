package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author Blue红红
 * @description home页面和good_detail页面的独立访客数
 * @create 2022/6/22 15:34
 */
public class DwsTrafficPageViewWindow {

    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //启用状态后端
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.setRestartStrategy(
//                RestartStrategies.failureRateRestart(3, Time.days(1L), Time.minutes(3L))
//        );
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
//        System.setProperty("HADOOP_USER_NAME", "atguigu");


        // TODO 2.读取kafka，获取topic:dwd_traffic_page_log的数据
        String topic = "dwd_traffic_page_log";
        String groupId = "page_view_window_1227";
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtil.getFlinkConsumer(topic, groupId));

        // TODO 3.先转换为JSON，再过滤出page里pageId包含home和good_detail的数据
        SingleOutputStreamOperator<JSONObject> homeAndDetailDS = dataStreamSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                if ("home".equals(pageId) || "good_detail".equals(pageId)) {
                    out.collect(jsonObject);
                }
            }
        });

        // TODO 4.提取时间戳生成watermark
        SingleOutputStreamOperator<JSONObject> homeAndDetailWithWmDS = homeAndDetailDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        // TODO 5.分组
        KeyedStream<JSONObject, String> keyedStream = homeAndDetailDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        // TODO 6.去除同一页面浏览的重复的人
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> homeOrDetailToBeanDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            private ValueState<String> homeLastDateState;
            private ValueState<String> detailLastDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> homeStateDescriptor = new ValueStateDescriptor<>("home", String.class);
                ValueStateDescriptor<String> detailStateDescriptor = new ValueStateDescriptor<>("detail", String.class);

                // 设置状态存活时间
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                homeStateDescriptor.enableTimeToLive(ttlConfig);
                detailStateDescriptor.enableTimeToLive(ttlConfig);
                homeLastDateState = getRuntimeContext().getState(homeStateDescriptor);
                detailLastDateState = getRuntimeContext().getState(detailStateDescriptor);

            }

            @Override
            public void flatMap(JSONObject value, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {

                String homeLastDt = homeLastDateState.value();
                String detailLastDt = detailLastDateState.value();
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.toDate(ts);

                // home页面访问人数统计，detail页面访问人数统计
                long homeUvCt = 0L;
                long goodDetailUvCt = 0L;
                String pageId = value.getJSONObject("page").getString("pageId");
                if ("home".equals(pageId)) {
                    if (homeLastDt == null || !homeLastDt.equals(curDt)) {
                        homeLastDateState.update(curDt);
                        homeUvCt = 1L;
                    }
                } else {
                    if (detailLastDt == null || !detailLastDt.equals(curDt)) {
                        detailLastDateState.update(curDt);
                        goodDetailUvCt = 1L;
                    }
                }
                if (homeUvCt == 1L || goodDetailUvCt == 1L) {
                    TrafficHomeDetailPageViewBean homeDetailPageViewBean = new TrafficHomeDetailPageViewBean("", "", homeUvCt, goodDetailUvCt, ts);
                    out.collect(homeDetailPageViewBean);
                }
            }
        });

        // TODO 7.开窗 聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceDS = homeOrDetailToBeanDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        TrafficHomeDetailPageViewBean next = values.iterator().next();
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setTs(System.currentTimeMillis()); // 作为ck的去重版本字段
                        out.collect(next);
                    }
                });

        // TODO 8.写入ck
        reduceDS.print(">>>>>>>>>>>");
        reduceDS.addSink(MyClickHouseUtil.getSink("insert into dws_traffic_page_view_window values(?,?,?,?,?)"));

        env.execute("DwsTrafficPageViewWindow");


    }
}
