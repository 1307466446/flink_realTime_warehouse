package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author Blue红红
 * @description 通过 版本，渠道，地区，访客类别 分组，计算会话数，页面浏览数，浏览时长，独立访客数，跳出会话数
 * @create 2022/6/23 13:50
 */
public class DwsTrafficVcChArIsNewPageViewWindow {

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

        // TODO 2.读取 dwd_traffic_page_log，dwd_traffic_unique_visitor_detail，dwd_traffic_user_jump_detail 主题
        String pageTopic = "dwd_traffic_page_log";
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        String ujTopic = "dwd_traffic_user_jump_detail";
        String groupId = "channel_page_view_window_1227";

        DataStreamSource<String> pageDS = env.addSource(MyKafkaUtil.getFlinkConsumer(pageTopic, groupId));
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getFlinkConsumer(uvTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getFlinkConsumer(ujTopic, groupId));


        // TODO 3.分别将3条流的数据转换 JavaBean类型
        // vc ch ar is_new   会话数 页面浏览数  浏览时间  
        SingleOutputStreamOperator<TrafficPageViewBean> pageBeanDS = pageDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject page = jsonObject.getJSONObject("page");
            JSONObject common = jsonObject.getJSONObject("common");
            String last_page_id = page.getString("last_page_id");
            // 页面浏览数 来一条page就是1
            Long during_time = page.getLong("during_time");

            long svCt = 0L;
            if (last_page_id == null) {
                svCt = 1L;
            }
            TrafficPageViewBean trafficPageViewBean = new TrafficPageViewBean("", "", common.getString("vc"), common.getString("ch"), common.getString("ar"),
                    common.getString("is_new"), 0L, svCt, 1L, during_time, 0L, jsonObject.getLong("ts"));
            return trafficPageViewBean;

        });

        // 独立访客 uv
        SingleOutputStreamOperator<TrafficPageViewBean> uvBeanDS = uvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            TrafficPageViewBean trafficPageViewBean = new TrafficPageViewBean("", "", common.getString("vc"), common.getString("ch"), common.getString("ar"),
                    common.getString("is_new"), 1L, 0L, 0L, 0L, 0L, jsonObject.getLong("ts"));
            return trafficPageViewBean;
        });

        // 跳出会话
        SingleOutputStreamOperator<TrafficPageViewBean> ujBeanDS = ujDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            TrafficPageViewBean trafficPageViewBean = new TrafficPageViewBean("", "", common.getString("vc"), common.getString("ch"), common.getString("ar"),
                    common.getString("is_new"), 0L, 0L, 0L, 0L, 1L, jsonObject.getLong("ts"));
            return trafficPageViewBean;
        });

        // TODO 4.合并3条流
        // 因为三条流的数据类型都是一样的，javaBean类型
        DataStream<TrafficPageViewBean> unionDS = pageBeanDS.union(uvBeanDS, ujBeanDS);

        // TODO 5.提取时间戳以及watermark
        // 这里的乱序程度设置是有讲究的
        // 因为超时数据会被输出到侧输出流
        // uj中窗口10s，延迟2s，假设topic_log在ts=11s来了条数，在uj中至少ts=   23s时这条数据才能输出
        // 而ts=22s时我们的这个程序【10，20）的窗口关闭了，超时数据自然进不来，ts=23时，超时数据会到我们这个窗口中来，这是来的数据是ts=11s的所以我们这里窗口延迟关闭时间至少是13s。
        SingleOutputStreamOperator<TrafficPageViewBean> unionWithWmDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(13))
                        .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                            @Override
                            public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));

        // TODO 6.分组 开窗

        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowedStream = unionWithWmDS.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean value) throws Exception {  

                return Tuple4.of(value.getVc(), value.getCh(), value.getAr(), value.getIsNew());
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)));


        // TODO 7.聚合
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = windowedStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                return value1;
            }
        }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {

                // 一个分组后的窗口聚合后的数据
                TrafficPageViewBean pageViewBean = input.iterator().next();
                pageViewBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                pageViewBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                pageViewBean.setTs(System.currentTimeMillis());
                out.collect(pageViewBean);
            }
        });


        reduceDS.print(">>>>>>>>>");
        reduceDS.addSink(MyClickHouseUtil.getSink("insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"));


        env.execute("DwsTrafficVcChArIsNewPageViewWindow");

    }
}
