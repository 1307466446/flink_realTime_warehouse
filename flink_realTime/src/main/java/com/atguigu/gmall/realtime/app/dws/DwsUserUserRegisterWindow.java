package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.atguigu.gmall.realtime.bean.UserRegisterBean;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

//数据流：web/app -> nginx -> 业务服务器 -> Mysql(Binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程  序：Mock -> Mysql(Binlog) -> Maxwell -> Kafka(ZK) -> DwdUserRegister -> Kafka(ZK) -> DwsUserUserRegisterWindow -> ClickHouse(ZK)
public class DwsUserUserRegisterWindow {

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

        //TODO 2.读取Kafka DWD层用户注册主题数据创建流
        String topic = "dwd_user_register";
        String groupId = "user_register_window_1227";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkConsumer(topic, groupId));

        //TODO 3.转换为JavaBean对象
        SingleOutputStreamOperator<UserRegisterBean> userRegisterDS = kafkaDS.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);
            return new UserRegisterBean("",
                    "",
                    1L,
                    DateFormatUtil.toTs(jsonObject.getString("create_time"), true));
        });

        //TODO 4.提取时间戳生成Watermark
        SingleOutputStreamOperator<UserRegisterBean> userRegisterWithWmDS = userRegisterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
            @Override
            public long extractTimestamp(UserRegisterBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //TODO 5.开窗、聚合
        SingleOutputStreamOperator<UserRegisterBean> resultDS = userRegisterWithWmDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                    @Override
                    public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                        value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                        return value1;
                    }
                }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserRegisterBean> values, Collector<UserRegisterBean> out) throws Exception {

                        //获取数据
                        UserRegisterBean userRegister = values.iterator().next();

                        //补充信息
                        userRegister.setTs(System.currentTimeMillis());
                        userRegister.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        userRegister.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                        //输出数据
                        out.collect(userRegister);
                    }
                });

        //TODO 6.将数据写出
        resultDS.print(">>>>>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSink("insert into dws_user_user_register_window values(?,?,?,?)"));

        //TODO 7.启动
        env.execute("DwsUserUserRegisterWindow");

    }

}
