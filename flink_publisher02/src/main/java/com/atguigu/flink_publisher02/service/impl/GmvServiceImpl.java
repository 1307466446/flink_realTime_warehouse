package com.atguigu.flink_publisher02.service.impl;


import com.atguigu.flink_publisher02.mapper.GmvMapper;
import com.atguigu.flink_publisher02.service.GmvService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public class GmvServiceImpl implements GmvService {

    @Autowired
    private GmvMapper gmvMapper;

    @Override
    public BigDecimal getGmv(int date) {
        return gmvMapper.selectGmv(date);
    }
}
