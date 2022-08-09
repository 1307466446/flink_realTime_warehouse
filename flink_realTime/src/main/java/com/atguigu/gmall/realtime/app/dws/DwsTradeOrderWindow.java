package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TradeOrderBean;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author Blue红红
 * @description 功能描述
 * @create 2022/6/25 23:59
 */
public class DwsTradeOrderWindow {
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


        //TODO 2.读取Kafka DWD层下单主题数据创建流
        String topic = "dwd_trade_order_detail";
        String groupId = "order_window_1227";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkConsumer(topic, groupId));

        // TODO 3.转换为JSON
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(JSONObject::parseObject);

        // TODO 3.按照order_detail_id分组，准备去除left join产生的未关联字段为null的数据
        KeyedStream<JSONObject, String> detailKeyedDS = jsonDS.keyBy(json -> json.getString("id"));

        // TODO 4. 去重 left join产生的重复数据  用状态+定时器
        SingleOutputStreamOperator<JSONObject> orderDetailDS = detailKeyedDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<JSONObject> lastOrderDetailState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastOrderDetailState = getRuntimeContext().getState(new ValueStateDescriptor<>("last_detail", JSONObject.class));

            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject lastDetailValue = lastOrderDetailState.value();

                if (lastDetailValue == null) {
                    lastOrderDetailState.update(value);
                    long currentProcessingTime = ctx.timerService().currentProcessingTime();
                    ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                } else {
                    if (value.getString("row_op_ts").compareTo(lastDetailValue.getString("row_op_ts")) >= 0) {
                        lastOrderDetailState.update(value);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                out.collect(lastOrderDetailState.value());
                lastOrderDetailState.clear();
            }
        });

        // TODO 主线任务： 下单独立用户数 和 首次下单用户数  以及金额的和
        // TODO 5. 提取时间戳和watermark
        SingleOutputStreamOperator<JSONObject> detailDSWithWmDS = orderDetailDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return DateFormatUtil.toTs(element.getString("create_time"), true);
                    }
                }));

        // TODO 6.分组
        KeyedStream<JSONObject, String> keyedDS = detailDSWithWmDS.keyBy(json -> json.getString("user_id"));

        // TODO 7. 求当日下单独立用户数 和 首次下单用户数
        // 因为我们要求用户的金额，所以对于重复的数据我们不能过滤
        SingleOutputStreamOperator<TradeOrderBean> detailBeanDS = keyedDS.map(new RichMapFunction<JSONObject, TradeOrderBean>() {

            private ValueState<String> lastOrderDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastOrderDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last_orderDt", String.class));
            }

            @Override
            public TradeOrderBean map(JSONObject value) throws Exception {
                String last_value = lastOrderDtState.value();
                String current_time = value.getString("create_time").split(" ")[0];

                // 下单独立用户数
                long orderUniqueUserCount = 0L;
                // 下单新用户数
                long orderNewUserCount = 0L;
                if (last_value == null) {
                    lastOrderDtState.update(current_time);
                    orderUniqueUserCount = 1L;
                    orderNewUserCount = 1L;
                } else {
                    if (!last_value.equals(current_time)) {
                        orderUniqueUserCount = 1L;
                        lastOrderDtState.update(current_time);
                    }
                }

                // 有些订单并未参加活动
                Double split_activity_amount = value.getDouble("split_activity_amount");
                if (split_activity_amount == null) {
                    split_activity_amount = 0.0D;
                }
                Double split_coupon_amount = value.getDouble("split_coupon_amount");
                if (split_coupon_amount == null) {
                    split_coupon_amount = 0.0D;
                }

                return new TradeOrderBean("", "", orderUniqueUserCount, orderNewUserCount, split_activity_amount, split_coupon_amount, value.getDouble("split_original_amount"), null);
            }
        });

        // TODO 8. 开窗，聚合
        SingleOutputStreamOperator<TradeOrderBean> reduceDS = detailBeanDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeOrderBean>() {
                    @Override
                    public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {
                        value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                        value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                        value1.setOrderCouponReduceAmount(value1.getOrderCouponReduceAmount() + value2.getOrderCouponReduceAmount());
                        value1.setOrderActivityReduceAmount(value1.getOrderActivityReduceAmount() + value2.getOrderActivityReduceAmount());
                        value1.setOrderOriginalTotalAmount(value1.getOrderOriginalTotalAmount() + value2.getOrderOriginalTotalAmount());
                        return value1;
                    }
                }, new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) throws Exception {
                        // 聚合后的数据
                        TradeOrderBean tradeOrderBean = values.iterator().next();
                        tradeOrderBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        tradeOrderBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        tradeOrderBean.setTs(System.currentTimeMillis());
                        out.collect(tradeOrderBean);

                    }
                });
        
        // TODO 9.
        reduceDS.print(">>>>>>>>>>>>");
        
        reduceDS.addSink(MyClickHouseUtil.getSink("insert into dws_trade_order_window values(?,?,?,?,?,?,?,?)"));
        
        env.execute("DwsTradeOrderWindow");
        


    }


}
