package com.atguigu.gmall.realtime.utils;

/**
 * @author Blue红红
 * @description 功能描述
 * @create 2022/6/19 16:32
 */
public class MysqlUtil {

    public static String getBaseDicLookUpDDL() {
        return "create table base_dic( " +
                "`dic_code` string, " +
                "`dic_name` string, " +
                "`parent_code` string, " +
                "`create_time` timestamp, " +
                "`operate_time` timestamp, " +
                "primary key(`dic_code`) not enforced " +
                ")" + mysqlLookUpTableDDL("base_dic");

    }

    public static String mysqlLookUpTableDDL(String tableName) {
        return "with ( " +
                "'connector' = 'jdbc', " +
                "'url' = 'jdbc:mysql://hadoop102:3306/gmall_1227_flink', " +
                "'table-name' = '" + tableName + "', " +
                "'lookup.cache.max-rows' = '10', " +
                "'lookup.cache.ttl' = '1 hour', " +
                "'username' = 'root', " +
                "'password' = 'root', " +
                "'driver' = 'com.mysql.cj.jdbc.Driver' " +
                ")";
    }
}
