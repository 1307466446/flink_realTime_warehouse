package com.atguigu.flink_publisher02.service.impl;


import com.atguigu.flink_publisher02.mapper.UvMapper;
import com.atguigu.flink_publisher02.service.UvService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class UvServiceImpl implements UvService {

    @Autowired
    private UvMapper uvMapper;

    @Override
    public Map getUvByCh(int date, int limit) {

        //查询数据
        List<Map> list = uvMapper.selectUvByCh(date, limit);

        //创建Map用于存放最终结果数据
        HashMap<String, BigInteger> result = new HashMap<>();

        //遍历集合,将数据存入Map中

        for (Map map : list) {
            result.put((String) map.get("ch"), (BigInteger) map.get("uv"));
        }

        //返回结果
        return result;
    }
}
