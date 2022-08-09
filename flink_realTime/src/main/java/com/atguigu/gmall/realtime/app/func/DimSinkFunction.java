package com.atguigu.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.DimUtil;
import com.atguigu.gmall.realtime.utils.DruidDSUtil;
import com.atguigu.gmall.realtime.utils.JedisPoolUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Set;

/**
 * @author Blue红红
 * @description 自定义sink，将主流中的数据写入到hbase中不同的维度表
 * 因为JDBCSink一次只能写一张表，所以我们得自定义sink
 * @create 2022/6/14 20:36
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    // 因为要写入第一步就是连接hbase
    private DruidDataSource dataSource;
    private JedisPool jedisPool;

    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource = DruidDSUtil.createDataSource();
        jedisPool = JedisPoolUtil.getJedisPool();
    }

    /*
    value: {
        "database": "gmall",
        "table": "base_trademark",
        "type": "insert",
        "ts": 1592270938,
        "xid": 13090,
        "xoffset": 1573,
        "data": {
            "id": "12",
            "tm_name": "atguigu"
        },
        "sinkTable": "dim_base_trademark"  是我们后来添加的，就是为了要知道数据往那张表里写
    }
    */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");
        String upsertSql = genUpsertSql(data, sinkTable);
        DruidPooledConnection connection = dataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(upsertSql);
        // 当数据发生改变时，删除redis中缓存的旧数据
        if ("update".equals(value.getString("type"))) {
            Jedis jedis = jedisPool.getResource();
            DimUtil.delRedisDimInfo(jedis, sinkTable.toUpperCase(), data.getString("id"));
            jedis.close();
        }
        preparedStatement.execute();
        connection.commit(); // 往hbase写注意！！！要加commit


        preparedStatement.close();
        connection.close();

    }

    /**
     * @param data
     * @param sinkTable
     * @return upsert into db.dim_base_trademark(id,tm_name) values('12','atguigu')
     */
    private String genUpsertSql(JSONObject data, String sinkTable) {
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(columns, ",") + ") values ('" +
                StringUtils.join(values, "','") + "')";
        System.out.println("插入的SQL：" + sql);
        return sql;
    }
}
