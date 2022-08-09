package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.AsyncDimFunction;
import com.atguigu.gmall.realtime.bean.TradeTrademarkCategoryUserRefundBean;
import com.atguigu.gmall.realtime.bean.TradeTrademarkCategoryUserSpuOrderBean;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
 * @description 交易域品牌-品类-用户-粒度退单各窗口  退单数
 * @create 2022/6/28 18:44
 */
public class DwsTradeTrademarkCategoryUserRefundWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
//        env.enableCheckpointing(3*60000L);
//        env.getCheckpointConfig().setCheckpointTimeout(5*60000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);


        // TODO 1.消费topic：dwd_trade_order_refund数据
        String topic = "dwd_trade_order_refund";
        String groupId = "TrademarkCategoryUserRefundWindow_1227";
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtil.getFlinkConsumer(topic, groupId));

        // TODO 2.将数据转换为javaBean
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanDS = dataStreamSource.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            HashSet<String> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getString("order_id"));
            return TradeTrademarkCategoryUserRefundBean.builder()
                    .userId(jsonObject.getString("user_id"))
                    .skuId(jsonObject.getString("sku_id"))
                    .orderIdSet(orderIds)
                    .ts(DateFormatUtil.toTs(jsonObject.getString("create_time"), true))
                    .build();
        });

        // TODO: 3.提取时间戳和wm
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanWithWmDS = beanDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public long extractTimestamp(TradeTrademarkCategoryUserRefundBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        // TODO 4.关联维表 补充与分组相关的字段
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanWithTmC3DS = AsyncDataStream.unorderedWait(beanWithWmDS,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>("dim_sku_info") {
                    @Override
                    protected String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getSkuId();
                    }

                    @Override
                    protected void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setTrademarkId(dimInfo.getString("tm_id".toUpperCase()));
                        input.setCategory3Id(dimInfo.getString("category3_id".toUpperCase()));

                    }
                }, 100, TimeUnit.SECONDS);

        // TODO 5.分组 开窗 聚合
        KeyedStream<TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>> keyedStream = beanWithTmC3DS.keyBy(new KeySelector<TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> getKey(TradeTrademarkCategoryUserRefundBean value) throws Exception {
                return Tuple3.of(value.getUserId(), value.getCategory3Id(), value.getTrademarkId());
            }
        });

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceDS = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                }, new WindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple3<String, String, String> stringStringStringTuple3, TimeWindow window, Iterable<TradeTrademarkCategoryUserRefundBean> input, Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {
                        TradeTrademarkCategoryUserRefundBean refundBean = input.iterator().next();
                        refundBean.setRefundCount((long) refundBean.getOrderIdSet().size());
                        refundBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        refundBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        out.collect(refundBean);
                    }
                });

        // TODO 6.继续关联维表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceWithTmDS = AsyncDataStream.unorderedWait(reduceDS,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>("dim_base_trademark") {
                    @Override
                    protected String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    protected void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setTrademarkName(dimInfo.getString("tm_name".toUpperCase()));
                    }
                }, 100, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceWithTmC3DS = AsyncDataStream.unorderedWait(reduceWithTmDS,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>("dim_base_category3") {
                    @Override
                    protected String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory3Id();
                    }

                    @Override
                    protected void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory3Name(dimInfo.getString("name".toUpperCase()));
                        input.setCategory2Id(dimInfo.getString("category2_id".toUpperCase()));
                    }
                }, 100, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceWithTmC3C2DS = AsyncDataStream.unorderedWait(reduceWithTmC3DS,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>("dim_base_category2") {
                    @Override
                    protected String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    protected void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory2Name(dimInfo.getString("name".toUpperCase()));
                        input.setCategory1Id(dimInfo.getString("category1_id".toUpperCase()));
                    }
                }, 100, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceWithTmC3C2C1DS = AsyncDataStream.unorderedWait(reduceWithTmC3C2DS,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>("dim_base_category1") {
                    @Override
                    protected String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    protected void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory1Name(dimInfo.getString("name".toUpperCase()));
                    }
                }, 100, TimeUnit.SECONDS);
        
        // TODO 7.
        reduceWithTmC3C2C1DS.print("reduceWithTmC3C2C1DS>>>>>>>>>");
        reduceWithTmC3C2C1DS.addSink(MyClickHouseUtil.getSink("insert into dws_trade_trademark_category_user_refund_window values(?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute("DwsTradeTrademarkCategoryUserRefundWindow");


    }
}
