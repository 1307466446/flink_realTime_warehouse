package com.atguigu.app;

import com.atguigu.bean.Bean1;
import com.atguigu.bean.Bean2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * @author Blue红红
 * @description 功能描述
 * @create 2022/6/17 20:35
 */
public class Flink_IntervalJoin {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Bean1> bean1DS = env.socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean1(fields[0], fields[1], Long.parseLong(fields[2]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Bean1>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean1>() {
                    @Override
                    public long extractTimestamp(Bean1 element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        SingleOutputStreamOperator<Bean2> bean2DS = env.socketTextStream("localhost", 8888)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean2(fields[0], fields[1], Long.parseLong(fields[2]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Bean2>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean2>() {
                    @Override
                    public long extractTimestamp(Bean2 element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        SingleOutputStreamOperator<Tuple2<Bean1, Bean2>> process = bean1DS.keyBy(Bean1::getId)
                .intervalJoin(bean2DS.keyBy(Bean2::getId))
                .between(Time.milliseconds(-2), Time.milliseconds(1))
                .process(new ProcessJoinFunction<Bean1, Bean2, Tuple2<Bean1, Bean2>>() {
                    @Override
                    public void processElement(Bean1 left, Bean2 right, Context ctx, Collector<Tuple2<Bean1, Bean2>> out) throws Exception {
                        out.collect(Tuple2.of(left, right));
                    }
                });

        process.print(">>>>>>>");

        env.execute();
    }
}
