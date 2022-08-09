package com.atguigu.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.DruidDSUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Blue红红
 * @description 处理两条流
 * @create 2022/6/14 14:50
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    MapStateDescriptor<String, TableProcess> map_state;
    private DruidDataSource druidDataSource;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> map_state) {
        this.map_state = map_state;
    }

    public TableProcessFunction() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }

    //value:{"before":null,"after":{"source_table":"base_trademark","sink_table":"dim_base_trademark","sink_columns":"id,tm_name","sink_pk":"id","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1655172926148,"snapshot":"false","db":"gmall-211227-config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1655172926150,"transaction":null}
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        // 主要任务：将flinkCDC读取的配置表数据，写入状态广播到主流，且在phoenix中建表
        JSONObject jsonObject = JSONObject.parseObject(value);
        String after = jsonObject.getString("after");
        if (after != null) {
            // 二话不说，先转tableProcess
            TableProcess tableProcess = JSON.parseObject(after, TableProcess.class);
            // 建表
            checkTable(tableProcess.getSinkTable(), tableProcess.getSinkPk(), tableProcess.getSinkColumns(), tableProcess.getSinkExtend());
            // 将数据写入状态，先获取状态
            BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(map_state);
            // 将数据写入状态，用来给主流匹配维度表的数据
            broadcastState.put(tableProcess.getSourceTable(), tableProcess);
        }
    }

//    {
//          "database":"gmall","table":"cart_info","type":"update","ts":1592270938,"xid":13090,"xoffset":1573,
//          "data":
//                  {
//                      "id":100924,"user_id":"93","sku_id":16,"cart_price":4488.00,"sku_num":1,
//                      "img_url":"http://47.93.148.192:8080/group1/M00/00/02/rBHu8l-sklaALrngAAHGDqdpFtU741.jpg",
//                      "sku_name":"华为 HUAWEI P40 麒麟990 5G SoC芯片 5000万超感知徕卡三摄 30倍数字变焦 8GB+128GB亮黑色全网通5G手机",
//                      "is_checked":null,"create_time":"2020-06-14 09:28:57","operate_time":null,"is_ordered":1,
//                      "order_time":"2021-10-17 09:28:58","source_type":"2401","source_id":null
//                  },
//          "old":
//                  {
//                      "is_ordered":0,
//                      "order_time":null
//                   }
//     }
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 获取广播流的·数据,map_state中的sourceTable
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(map_state);
        // 主要任务：筛选出维度表的相关数据并写入hbase对应的表中
        // 通过主流的数据拿到table，通过table去从广播流中取值
        String key = value.getString("table");
        String type = value.getString("type");
        // 获取这条数据对应要写入的hbase的tableProcess
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null && ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type))) {
            // 过滤出要写到hbase中的字段，因为并不是所有的字段都要写入hbase的
            // 这里我们写出的是JSONObject类型，好获取里面的字段
            // 过滤出我们需要的字段，ods层中有些数据是我们不需要的
            filterColumns(value.getJSONObject("data"), tableProcess.getSinkColumns());

            // 把sinkTable写入value，我们要将value写入到hbase中的那个表
            value.put("sinkTable", tableProcess.getSinkTable());
            out.collect(value);

        } else if (tableProcess == null) {
            System.out.println("配置信息中不存在该维度表：" + key);
        }
    }

    /**
     * @param data        {"id":"12", "tm_name":"nike", "logo_url":"xxx.xxx"}
     * @param sinkColumns "id, tm_name"
     */
    private void filterColumns(JSONObject data, String sinkColumns) {
        // JSONObject和map的处理一样 entrySet keySet ......
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);

        // 每一个键值对组成一个entry实体
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(entry -> !columnList.contains(entry.getKey()));
    }


    //拼接SQL语句 create table if not exists db.tn(id varchar primary key,name varchar,sex varchar) xxx
    private void checkTable(String sinkTable, String sinkPk, String sinkColumns, String sinkExtend) {
        if (sinkPk == null || sinkPk.equals("")) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        StringBuilder createTableSql = new StringBuilder("create table if not exists ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");
        String[] columns = sinkColumns.split(",");
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            if (sinkPk.equals(column)) {
                createTableSql.append(column).append(" varchar primary key");
            } else {
                createTableSql.append(column).append(" varchar");
            }

            if (i < columns.length - 1) {
                createTableSql.append(",");
            }
        }
        createTableSql.append(")");
        createTableSql.append(sinkExtend);

//        System.out.println("建表语句：" + createTableSql);

        DruidPooledConnection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            // 获取连接
            connection = druidDataSource.getConnection();
            // 编译sql
            preparedStatement = connection.prepareStatement(createTableSql.toString());
            // 执行sql
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表失败：" + sinkTable);
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }


    }
}
