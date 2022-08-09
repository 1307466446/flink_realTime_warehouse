package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.bean.TransientSink;
import com.atguigu.gmall.realtime.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Blue红红
 * @description 功能描述
 * @create 2022/6/21 20:50
 */
public class MyClickHouseUtil {
    /**
     * @param sql
     * @param <T> 这里是做类型的声明，SinkFunction<T> 不然这里以为类的名字是T，用返回值用T表示时，在前面一定要加申明
     * @return
     */
    public static <T> SinkFunction<T> getSink(String sql) {

        return JdbcSink.sink(sql, new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        Field[] declaredFields = t.getClass().getDeclaredFields();

                        int offset = 0;
                        for (int i = 0; i < declaredFields.length; i++) {
                            Field field = declaredFields[i];
                            field.setAccessible(true);
                            //对于我们在javaBean中写入时忽略的字段，我们使用类似@transient的注解，使用时正常使用，但写入时不写入
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                offset++;
                                continue;
                            }
                            Object value = field.get(t); //反射，通过属性获取对象 属性.get(对象)，正常我们是通过 对象.getXXX()属性
                            preparedStatement.setObject(i - offset + 1, value);
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchIntervalMs(1000)
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());

    }
}
