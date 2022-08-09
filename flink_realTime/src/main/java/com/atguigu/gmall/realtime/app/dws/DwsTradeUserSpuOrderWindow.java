package com.atguigu.gmall.realtime.app.dws;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.AsyncDimFunction;
import com.atguigu.gmall.realtime.bean.TradeTrademarkCategoryUserSpuOrderBean;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
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
 * @description 用户-spu粒度用户下单各窗口汇总表
 * @create 2022/6/26 15:34
 * 
 * 
 * 数据流：web/app -> Nginx -> 业务服务器 -> Mysql(Binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
 * 程  序：  Mock -> Mysql(Binlog) -> Maxwell -> Kafka(ZK) -> DwdTradeOrderPreProcess -> Kafka(ZK) -> DwdTradeOrderDetail -> Kafka(ZK)
 */
public class DwsTradeUserSpuOrderWindow {

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


        String topic = "dwd_trade_order_detail";
        String groupId = "user_spu_order_window_211227";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkConsumer(topic, groupId));

        //TODO 3.转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSONObject::parseObject);

        //TODO 4.按照订单明细ID分组
        KeyedStream<JSONObject, String> keyedByDetailIdDS = jsonObjDS.keyBy(json -> json.getString("id"));

        //TODO 5.使用状态编程+定时器的方式获取时间最大的一条数据
        SingleOutputStreamOperator<JSONObject> filterDS = keyedByDetailIdDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<JSONObject> orderDetailState;

            @Override
            public void open(Configuration parameters) throws Exception {
                orderDetailState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("order-state", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

                //取出状态数据
                JSONObject lastOrderDetail = orderDetailState.value();

                //判断状态数据是否为null
                if (lastOrderDetail == null) {
                    //说明为第一条数据,将当前数据保存至状态,同时注册定时器
                    orderDetailState.update(value);
                    long currentPt = ctx.timerService().currentProcessingTime();
                    ctx.timerService().registerProcessingTimeTimer(currentPt + 5000L);
                } else {
                    //说明不是第一条数据,则需要比较状态中数据与当前数据的时间大小
                    if (value.getString("row_op_ts").compareTo(lastOrderDetail.getString("row_op_ts")) >= 0) {
                        orderDetailState.update(value);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {

                //取出状态数据并输出,注意需要清空状态
                JSONObject value = orderDetailState.value();
                out.collect(value);
                orderDetailState.clear();
            }
        });

        //TODO 6.转换数据为JavaBean对象
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> tradeTrademarkCategoryUserSpuOrderDS = filterDS.map(json -> {
            HashSet<String> orderIds = new HashSet<>();
            orderIds.add(json.getString("order_id"));

            return TradeTrademarkCategoryUserSpuOrderBean.builder()
                    .userId(json.getString("user_id"))
                    .skuId(json.getString("sku_id"))
                    .orderIdSet(orderIds)
                    .orderAmount(json.getDouble("split_total_amount"))
                    .ts(DateFormatUtil.toTs(json.getString("create_time"), true))
                    .build();
        });

        //TODO 7.提取时间戳生成WaterMark
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> beanWithWmDS = tradeTrademarkCategoryUserSpuOrderDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeTrademarkCategoryUserSpuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserSpuOrderBean>() {
            @Override
            public long extractTimestamp(TradeTrademarkCategoryUserSpuOrderBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 8.关联维表 补充与分组相关的信息
        // 通过sku_id关联sku_info获取到tm_id,category3_id,spu_id，这四个粒度(user_id)就可以决定分组了，
//        beanWithWmDS.map(new RichMapFunction<TradeTrademarkCategoryUserSpuOrderBean, TradeTrademarkCategoryUserSpuOrderBean>() {
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                // 初始化连接池
//            }
//
//            @Override
//            public TradeTrademarkCategoryUserSpuOrderBean map(TradeTrademarkCategoryUserSpuOrderBean value) throws Exception {
//                //1.拼接要查询的sql
//                
//                //2.预编译sql
//                
//                //3.执行查询
//                
//                //4.补充信息
//                
//                return null;
//            }
//        });
        // map因不支持多线程处理，关联维表只能来一条数据进行一次查询关联，关联完后，才能关联下一条数据
        // 这样的效率是不够，我们采用多线程进行维表关联
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> userSpuDS = AsyncDataStream.unorderedWait(beanWithWmDS, new AsyncDimFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_SKU_INFO") {
                    @Override
                    protected String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getSkuId();
                    }

                    @Override
                    protected void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        input.setSpuId(dimInfo.getString("SPU_ID"));
                        input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                        input.setTrademarkId(dimInfo.getString("TM_ID"));

                    }
                },
                100, TimeUnit.SECONDS);

        userSpuDS.print("userSpuDS>>>>>>>>>>>>>>>>");

        // TODO 9.按照关联好的字段 粒度进行分组   开窗   聚合  
        // userId,spuId,category3Id,trademarkId
        KeyedStream<TradeTrademarkCategoryUserSpuOrderBean, Tuple4<String, String, String, String>> keyedStream = userSpuDS.keyBy(new KeySelector<TradeTrademarkCategoryUserSpuOrderBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TradeTrademarkCategoryUserSpuOrderBean value) throws Exception {

                return Tuple4.of(value.getUserId(), value.getSpuId(), value.getCategory3Id(), value.getTrademarkId());
            }
        });
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> reduceDS = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeTrademarkCategoryUserSpuOrderBean>() {
                    @Override
                    public TradeTrademarkCategoryUserSpuOrderBean reduce(TradeTrademarkCategoryUserSpuOrderBean value1, TradeTrademarkCategoryUserSpuOrderBean value2) throws Exception {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        return value1;
                    }
                }, new WindowFunction<TradeTrademarkCategoryUserSpuOrderBean, TradeTrademarkCategoryUserSpuOrderBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> key, TimeWindow window, Iterable<TradeTrademarkCategoryUserSpuOrderBean> input, Collector<TradeTrademarkCategoryUserSpuOrderBean> out) throws Exception {
                        TradeTrademarkCategoryUserSpuOrderBean userSpuOrderBean = input.iterator().next();

                        userSpuOrderBean.setOrderCount((long) userSpuOrderBean.getOrderIdSet().size());
                        userSpuOrderBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        userSpuOrderBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        out.collect(userSpuOrderBean);
                    }
                });


        // TODO 10. 关联维表，补充与分组不相关的信息 5张维表
        // 拿reduceDS去异步关联维表
        // dim_sku_info
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> reduceWithSpuNameDS = AsyncDataStream.unorderedWait(reduceDS, new AsyncDimFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_SPU_INFO") {
            @Override
            protected String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                return input.getSpuId();
            }

            @Override
            protected void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                input.setSpuName(dimInfo.getString("SPU_NAME"));
            }
        }, 100, TimeUnit.SECONDS);

        // 关联 dim_base_trademark
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> reduceWithSpuNameAndTMDS = AsyncDataStream.unorderedWait(reduceWithSpuNameDS, new AsyncDimFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_TRADEMARK") {
            @Override
            protected String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                return input.getTrademarkId();
            }

            @Override
            protected void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                input.setTrademarkName(dimInfo.getString("TM_NAME"));
            }


        }, 100, TimeUnit.SECONDS);

        // 关联 DIM_BASE_CATEGORY3
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> reduceWithSpuTmC3DS = AsyncDataStream.unorderedWait(reduceWithSpuNameAndTMDS, new AsyncDimFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_CATEGORY3") {
            @Override
            protected String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                return input.getCategory3Id();
            }

            @Override
            protected void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                input.setCategory3Name(dimInfo.getString("NAME"));
                input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
            }
        }, 100, TimeUnit.SECONDS);

        // 关联 DIM_BASE_CATEGORY2
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> reduceWithSpuTmC3C2DS = AsyncDataStream.unorderedWait(reduceWithSpuTmC3DS, new AsyncDimFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_CATEGORY2") {
            @Override
            protected String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                return input.getCategory2Id();
            }

            @Override
            protected void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                input.setCategory2Name(dimInfo.getString("NAME"));
                input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));

            }
        }, 100, TimeUnit.SECONDS);

        // 关联 DIM_BASE_CATEGORY1

        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> reduceWithSpuTmC3C2C1DS = AsyncDataStream.unorderedWait(reduceWithSpuTmC3C2DS, new AsyncDimFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_CATEGORY1") {
            @Override
            protected String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                return input.getCategory1Id();
            }

            @Override
            protected void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                input.setCategory1Name(dimInfo.getString("NAME"));
            }
        }, 100, TimeUnit.SECONDS);


        reduceWithSpuTmC3C2C1DS.print("reduceWithSpuTmC3C2C1DS>>>>>>>>>>>>>>>>>>>>");
        reduceWithSpuTmC3C2C1DS.addSink(MyClickHouseUtil.getSink("insert into dws_trade_user_spu_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));






        env.execute("DwsTradeUserSpuOrderWindow");
    }
}
