package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.CartAddUuBean;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * @description 交易域每日加购独立用户数
 * @create 2022/6/24 13:12
 */
public class DwsTradeCartAddUuWindow {

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

        //
        String topic = "dwd_trade_cart_add";
        String groupId = "dws_cart_add_uu_window_1227";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkConsumer(topic, groupId));

        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(JSON::parseObject);

        // 加购数据有两种
        // 1.insert 2. update new > old 
        //  createTime  operateTime
        SingleOutputStreamOperator<JSONObject> jsonWithWmDS = jsonDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        String create_time = element.getString("create_time");
                        String operate_time = element.getString("operate_time");
                        if (operate_time != null) {
                            return DateFormatUtil.toTs(operate_time, true);
                        }
                        return DateFormatUtil.toTs(create_time, true);
                    }
                }));

        //

        KeyedStream<JSONObject, String> keyedStream = jsonDS.keyBy(json -> json.getString("user_id"));

        // 

        SingleOutputStreamOperator<CartAddUuBean> cartAddBeanDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {

            private ValueState<String> lastCartAddDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("cart_add", String.class);
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);
                lastCartAddDtState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void flatMap(JSONObject value, Collector<CartAddUuBean> out) throws Exception {
                String lastCartAddDt = lastCartAddDtState.value();
                String create_time = value.getString("create_time");
                String operate_time = value.getString("operate_time");
                String curDt = null;
                if (operate_time != null) {
                    curDt = operate_time.split(" ")[0];
                } else {
                    curDt = create_time.split(" ")[0];
                }

                if (lastCartAddDt == null || !lastCartAddDt.equals(curDt)) {
                    lastCartAddDtState.update(curDt);
                    out.collect(new CartAddUuBean("", "", 1L, null));
                }
            }
        });

        //
        SingleOutputStreamOperator<CartAddUuBean> reduceDS = cartAddBeanDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                        value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                        return value1;
                    }
                }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<CartAddUuBean> values, Collector<CartAddUuBean> out) throws Exception {
                        CartAddUuBean addUuBean = values.iterator().next();

                        addUuBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        addUuBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        addUuBean.setTs(System.currentTimeMillis());

                    }
                });

        // 
        reduceDS.print(">>>>>>>>>>>>>>");
        reduceDS.addSink(MyClickHouseUtil.getSink("insert into dws_trade_cart_add_uu_window values(?,?,?,?)"));


        env.execute("DwsTradeCartAddUuWindow");


    }
}
