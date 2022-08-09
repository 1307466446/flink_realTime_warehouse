package com.atguigu.gmall.realtime.utils;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import redis.clients.jedis.Jedis;
import java.sql.Connection;
import java.util.List;

/**
 * @author Blue红红
 * @description 用来查询
 * @create 2022/6/27 12:07
 */
public class DimUtil {

    /**
     * @param jedis
     * @param connection
     * @param tableName
     * @param key
     * @return
     * @throws Exception
     */
    public static JSONObject getDimInfo(Jedis jedis, Connection connection, String tableName, String key) throws Exception {

        // 先从redis中查询数据 (读缓存)
        String redisKey = "DIM:" + tableName + ":" + key;
        String value = jedis.get(redisKey);
        if (value != null) {
            jedis.expire(redisKey, 24 * 60 * 60);
            return JSON.parseObject(value);
        }

        // 从hbase中读取数据
        String sql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id = '" + key + "'";
        List<JSONObject> list = PhoenixUtil.query(connection, sql, JSONObject.class, false);
        JSONObject dimInfo = list.get(0);
        // 写入redis并设置过期时间
        jedis.set(redisKey, dimInfo.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);

        return dimInfo;

    }

    /**
     * 数据发生改变时，删除redis中缓存的旧数据
     */
    public static void delRedisDimInfo(Jedis jedis, String tableName, String key) {
        String redisKey = "DIM:" + tableName + ":" + key;
        jedis.del(redisKey);

    }
    


}
