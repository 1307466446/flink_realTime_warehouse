package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TradePaymentWindowBean;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
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
 * @description 统计 每日支付成功独立用户数 和 首次支付成功用户数
 * @create 2022/6/24 17:12
 * <p>
 * 用的是方案二：上游有left join 下游就要去重，但是右表的数据在我们这个需求中不需要，所以我们直接过滤掉多余的数据
 * <p>
 * 数据流：web/app -> Nginx -> 业务服务器 -> Mysql(Binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
 * 程  序：  Mock -> Mysql(Binlog) -> Maxwell -> Kafka(ZK) -> DwdTradeOrderPreProcess -> Kafka(ZK) -> DwdTradeOrderDetail -> Kafka(ZK) -> DwdTradePayDetailSuc -> Kafka(ZK) -> DwsTradePaymentSucWindow -> ClickHouse(ZK)
 */
public class DwsTradePaymentSucWindow {

    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        //env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        //env.getCheckpointConfig().enableExternalizedCheckpoints(
        //        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        //);
        //env.setRestartStrategy(RestartStrategies.failureRateRestart(
        //        3, Time.days(1), Time.minutes(1)
        //));
        //env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage(
        //        "hdfs://hadoop102:8020/ck"
        //);
        //System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.读取Kafka DWD层成功支付主题数据创建流
        String topic = "dwd_trade_pay_detail_suc";
        String groupId = "payment_suc_window_1227";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkConsumer(topic, groupId));

        //TODO 3.将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //TODO 4.按照 order_detail_id 分组
        KeyedStream<JSONObject, String> keyedByDetailIdDS = jsonObjDS.keyBy(json -> json.getString("order_detail_id"));

        //TODO 5.由于不需要右表的数据,则取第一条数据即可(是由于left join产生的重复数据)
        SingleOutputStreamOperator<JSONObject> filterDS = keyedByDetailIdDS.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("value-state", String.class);
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(10))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);

                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //取出状态数据并判断是否为null
                String state = valueState.value();
                if (state == null) {
                    valueState.update("1");
                    return true;
                } else {
                    return false;
                }
            }
        });

        //TODO 6.提取时间戳生成Watermark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = filterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return DateFormatUtil.toTs(element.getString("callback_time"), true);
            }
        }));

        //TODO 7.按照 user_id 分组
        KeyedStream<JSONObject, String> keyedByUidDS = jsonObjWithWmDS.keyBy(json -> json.getString("user_id"));

        //TODO 8.获取独立支付用户并转换数据为JavaBean对象
        SingleOutputStreamOperator<TradePaymentWindowBean> tradePaymentDS = keyedByUidDS.flatMap(new RichFlatMapFunction<JSONObject, TradePaymentWindowBean>() {

            private ValueState<String> lastPayState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastPayState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-pay", String.class));
            }

            @Override
            public void flatMap(JSONObject value, Collector<TradePaymentWindowBean> out) throws Exception {

                //取出状态日期以及当前数据日期
                String lastDt = lastPayState.value();
                String curDt = value.getString("callback_time").split(" ")[0];

                //定义当天支付用户数以及新增支付用户数
                long paymentSucUniqueUserCount = 0L;
                long paymentSucNewUserCount = 0L;

                if (lastDt == null) {
                    paymentSucUniqueUserCount = 1L;
                    paymentSucNewUserCount = 1L;
                    lastPayState.update(curDt);
                } else if (!lastDt.equals(curDt)) {
                    paymentSucUniqueUserCount = 1L;
                    lastPayState.update(curDt);
                }

                //判断并输出数据
                if (paymentSucUniqueUserCount == 1L) {
                    out.collect(new TradePaymentWindowBean("", "",
                            paymentSucUniqueUserCount,
                            paymentSucNewUserCount,
                            null));
                }
            }
        });

        //TODO 9.开窗、聚合
        SingleOutputStreamOperator<TradePaymentWindowBean> resultDS = tradePaymentDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TradePaymentWindowBean>() {
                    @Override
                    public TradePaymentWindowBean reduce(TradePaymentWindowBean value1, TradePaymentWindowBean value2) throws Exception {
                        value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                        value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                        return value1;
                    }
                }, new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradePaymentWindowBean> values, Collector<TradePaymentWindowBean> out) throws Exception {

                        TradePaymentWindowBean tradePaymentWindowBean = values.iterator().next();

                        tradePaymentWindowBean.setTs(System.currentTimeMillis());
                        tradePaymentWindowBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        tradePaymentWindowBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                        out.collect(tradePaymentWindowBean);
                    }
                });

        //TODO 10.将数据写出
        resultDS.print(">>>>>>>>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSink("insert into dws_trade_payment_suc_window values(?,?,?,?,?)"));

        //TODO 11.启动任务
        env.execute("DwsTradePaymentSucWindow");


    }
}
