package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.AsyncDimFunction;
import com.atguigu.gmall.realtime.bean.TradeProvinceOrderWindow;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author Blue红红
 * @description 交易域省份粒度下单各窗口汇总表，计算各省份各窗口的订单数和订单金额
 * @create 2022/6/28 12:28
 */
public class DwsTradeProvinceOrderWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        System.setProperty("HADOOP_USER_NAME", "atguigu");
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
//        env.enableCheckpointing(3 * 60000L);
//        env.getCheckpointConfig().setCheckpointTimeout(5 * 60000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);

        // TODO 1.读取kafka topic：dwd_trade_order_detail
        String topic = "dwd_trade_order_detail";
        String groupId = "user_spu_order_window_211227";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkConsumer(topic, groupId));

        // TODO 2.转换为JSONObject对象
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(JSONObject::parseObject);

        // TODO 3.按订单Id分组，用于left join去重，状态+定时器
        // 其实我们这里还有另一种方法因为，因为右表的数据我们用不到，所以可以用第二种去重方法

        KeyedStream<JSONObject, String> keyedStream = jsonDS.keyBy(json -> json.getString("id"));

        SingleOutputStreamOperator<JSONObject> orderDetailDS = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<JSONObject> lastValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastValueState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("last_value", JSONObject.class));
            }


            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject lastValue = lastValueState.value();
                if (lastValue == null) {
                    lastValueState.update(value);
                    long currentProcessingTime = ctx.timerService().currentProcessingTime();
                    ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                } else {
                    if (value.getString("row_op_ts").compareTo(lastValue.getString("row_op_ts")) >= 0) {
                        lastValueState.update(value);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                out.collect(lastValueState.value());
                lastValueState.clear();
            }
        });


        // TODO 4.转换为javaBean对象
        SingleOutputStreamOperator<TradeProvinceOrderWindow> orderDetailBeanDS = orderDetailDS.map(json -> {
            HashSet<String> orderIds = new HashSet<>();
            orderIds.add(json.getString("order_id"));

            return TradeProvinceOrderWindow.builder()
                    .orderAmount(json.getDouble("split_total_amount"))
                    .provinceId(json.getString("province_id"))
                    .orderIdSet(orderIds)
                    .ts(DateFormatUtil.toTs(json.getString("create_time"), true))
                    .build();
        });

        // TODO 5.提取时间戳和watermark
        SingleOutputStreamOperator<TradeProvinceOrderWindow> detailBeanWithWmDS = orderDetailBeanDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeProvinceOrderWindow>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<TradeProvinceOrderWindow>() {
                    @Override
                    public long extractTimestamp(TradeProvinceOrderWindow element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        // TODO 6.分组 开窗 聚合
        SingleOutputStreamOperator<TradeProvinceOrderWindow> reduceDS = detailBeanWithWmDS.keyBy(TradeProvinceOrderWindow::getProvinceId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeProvinceOrderWindow>() {
                    @Override
                    public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow value1, TradeProvinceOrderWindow value2) throws Exception {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        return value1;
                    }
                }, new WindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderWindow> input, Collector<TradeProvinceOrderWindow> out) throws Exception {
                        TradeProvinceOrderWindow tradeProvinceOrderWindow = input.iterator().next();

                        tradeProvinceOrderWindow.setOrderCount((long) tradeProvinceOrderWindow.getOrderIdSet().size());
                        tradeProvinceOrderWindow.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        tradeProvinceOrderWindow.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        out.collect(tradeProvinceOrderWindow);
                    }
                });

        // TODO 7.关联维表 聚合数据减少关联维表效率高，异步关联效率更高
        SingleOutputStreamOperator<TradeProvinceOrderWindow> provinceOrderDS = AsyncDataStream.unorderedWait(reduceDS,
                new AsyncDimFunction<TradeProvinceOrderWindow>("dim_base_province") {
                    @Override
                    protected String getKey(TradeProvinceOrderWindow input) {
                        return input.getProvinceId();
                    }

                    @Override
                    protected void join(TradeProvinceOrderWindow input, JSONObject dimInfo) {
                        input.setProvinceName(dimInfo.getString("NAME"));
                    }
                }, 100, TimeUnit.SECONDS);

        // TODO 8.
        provinceOrderDS.print("provinceOrderDS>>>>>>>>>>>>>");
        provinceOrderDS.addSink(MyClickHouseUtil.getSink("insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)"));

        env.execute("DwsTradeProvinceOrderWindow");


    }


}
