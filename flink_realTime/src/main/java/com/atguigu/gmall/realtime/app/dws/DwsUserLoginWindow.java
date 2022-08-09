package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.UserLoginBean;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author Blue红红
 * @description 统计7日回流用户和当日独立用户
 * @create 2022/6/23 0:44
 */
public class DwsUserLoginWindow {
    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
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


        // TODO 2.读取Kafka DWD层 页面日志主题数据创建流
        String groupId = "user_login_window_1227";
        String pageTopic = "dwd_traffic_page_log";
        DataStreamSource<String> originalPageDS = env.addSource(MyKafkaUtil.getFlinkConsumer(pageTopic, groupId));

        // TODO 3.过滤出是登录的数据
        // 1.打开app需要登录的：uid != null && page_id = login  
        // 2.打开app自动登录的：uid != null && last_page_id == null
        SingleOutputStreamOperator<JSONObject> filterLoginDS = originalPageDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                JSONObject page = jsonObject.getJSONObject("page");
                String page_id = page.getString("page_id");
                String last_page_id = page.getString("last_page_id");
                String uid = jsonObject.getJSONObject("common").getString("uid");
                if (uid != null && ("login".equals(page_id) || last_page_id == null)) {
                    out.collect(jsonObject);
                }
            }
        });

        // TODO 4.提取时间戳生成watermark
        SingleOutputStreamOperator<JSONObject> filterLoginWithWmDS = filterLoginDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        // TODO 5.分组
        KeyedStream<JSONObject, String> keyedStream = filterLoginDS.keyBy(json -> json.getJSONObject("common").getString("uid"));

        // TODO 6.去重 取出独立用户 和 回流用户 转换成JavaBean对象
        SingleOutputStreamOperator<UserLoginBean> filterUserBeanDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, UserLoginBean>() {

            private ValueState<String> lastLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 因涉及到回流
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last_date", String.class);
                lastLoginDtState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void flatMap(JSONObject value, Collector<UserLoginBean> out) throws Exception {

                String lastLoginDt = lastLoginDtState.value();
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.toDate(ts);

                long uvCount = 0L;
                long backCount = 0L;
                if (lastLoginDt == null) {
                    // 新用户登录
                    lastLoginDtState.update(curDt);
                    uvCount = 1L;
                } else {
                    // 日期是否不为当前日期,不为一定是当日独立用户,也有可能是回流用户，回流用户一定是当日独立用户
                    if (!lastLoginDt.equals(curDt)) {
                        lastLoginDtState.update(curDt);
                        uvCount = 1L;
                        if (DateFormatUtil.toTs(curDt) - DateFormatUtil.toTs(lastLoginDt) > 7 * 24 * 60 * 60 * 1000) {
                            backCount = 1L;
                        }
                    }
                }

                if (uvCount == 1L) {
                    UserLoginBean userLoginBean = new UserLoginBean("", "", backCount, uvCount, ts);
                    out.collect(userLoginBean);
                }

            }
        });


        // TODO 7 开窗 聚合
        /**
         * 数据会先在reduce里面做聚合
         * 10s到之后
         * 聚合的数据会进入apply方法，所以apply里面只有一条结果数据
         */
        SingleOutputStreamOperator<UserLoginBean> reduceDS = filterUserBeanDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        return value1;
                    }
                }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                        UserLoginBean userLoginBean = values.iterator().next();
                        userLoginBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        userLoginBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        userLoginBean.setTs(System.currentTimeMillis());
                        out.collect(userLoginBean);
                    }
                });
        
        // TODO 8.数据写入ck
        reduceDS.addSink(MyClickHouseUtil.getSink("insert into dws_user_user_login_window values(?,?,?,?,?)"));
        
        env.execute("DwsUserLoginWindow");


    }
}
