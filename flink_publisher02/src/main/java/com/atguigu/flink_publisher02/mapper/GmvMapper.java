package com.atguigu.flink_publisher02.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

/**
 * @author Blue红红
 * @description 功能描述
 * @create 2022/6/29 11:02
 */
public interface GmvMapper {

    @Select("select sum(order_amount) order_amount from dws_trade_user_spu_order_window where toYYYYMMDD(stt)=#{date}")
    BigDecimal selectGmv(int date);
    
}
