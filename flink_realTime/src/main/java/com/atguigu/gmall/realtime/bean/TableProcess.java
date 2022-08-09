package com.atguigu.gmall.realtime.bean;

import lombok.Data;

/**
 * @author Blue红红
 * @description 功能描述
 * @create 2022/6/14 0:51
 */
@Data
public class TableProcess {
    //来源表
    String sourceTable;
    //输出表
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;
}