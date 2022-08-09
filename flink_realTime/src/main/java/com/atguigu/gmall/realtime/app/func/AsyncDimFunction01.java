package com.atguigu.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.DimUtil01;
import com.atguigu.gmall.realtime.utils.DruidDSUtil;
import com.atguigu.gmall.realtime.utils.JedisPoolUtil;
import com.atguigu.gmall.realtime.utils.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author Blue红红
 * @description 功能描述
 * @create 2022/6/29 0:46
 */
public abstract class AsyncDimFunction01<T> extends RichAsyncFunction<T, T> {

    private JedisPool jedisPool;
    private DruidDataSource dataSource;
    private ThreadPoolExecutor threadPoolExecutor;

    private String tableName;

    public AsyncDimFunction01(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        jedisPool = JedisPoolUtil.getJedisPool();
        dataSource = DruidDSUtil.createDataSource();
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.execute(new Runnable() {  // 注意这里使用线程
            @SneakyThrows
            @Override
            public void run() {
                Jedis jedis = jedisPool.getResource();
                DruidPooledConnection connection = dataSource.getConnection();

                String key = getKey(input);
                JSONObject dimInfo = DimUtil01.getDimInfo(jedis, connection, tableName, key);
                if (dimInfo != null) {
                    join(input, dimInfo);
                }

                resultFuture.complete(Collections.singleton(input));

                connection.close();
                jedis.close();
            }
        });


    }


    protected abstract String getKey(T input);

    protected abstract void join(T input, JSONObject dimInfo);


    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {

    }
}
