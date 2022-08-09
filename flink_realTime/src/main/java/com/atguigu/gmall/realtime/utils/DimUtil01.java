package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

/**
 * @author Blue红红
 * @description 功能描述
 * @create 2022/6/28 23:51
 */
public class DimUtil01 {

    public static JSONObject getDimInfo(Jedis jedis, Connection connection, String tableName, String key) throws Exception {
        String redisKey = "DIM:" + tableName + ":" + key;
        String value = jedis.get(redisKey);
        if (value != null) {
            jedis.expire(redisKey, 24 * 60 * 60);
            return JSONObject.parseObject(value);
        }


        // 查询数据库
        String sql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id = '" + key + "'";
        List<JSONObject> list = JDBCUtil.query(connection, sql, JSONObject.class, false);
        JSONObject jsonObject = list.get(0);
        jedis.set(redisKey, jsonObject.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        return jsonObject;

    }

    public static void delRedisDimInfo(Jedis jedis, String tableName, String key) {
        String redisKey = "DIM:" + tableName + ":" + key;
        jedis.del(redisKey);
    }

}
