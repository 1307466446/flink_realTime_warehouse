package com.atguigu.flink_publisher02.controller;


import com.atguigu.flink_publisher02.service.GmvService;
import com.atguigu.flink_publisher02.service.UvService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

//@Controller
@RestController
public class SugarController {

    @Autowired
    private GmvService gmvService;
    
    @Autowired
    private UvService uvService;

    @RequestMapping("test")
    //@ResponseBody
    public String test1() {
        System.out.println("111111111111111");
        return "success";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("name") String nn,
                        @RequestParam(value = "age", defaultValue = "18") int age) {
        System.out.println(nn + ":" + age);
        return "success";
    }

    @RequestMapping("api/sugar/gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date) {

        System.out.println("访问到了！！！");

        if (date == 0) {
            date = getToday();
        }

        //获取GMV数据
        BigDecimal gmv = gmvService.getGmv(date);

        //拼接字符串并返回
        return "{ " +
                "  \"status\": 0, " +
                "  \"msg\": \"\", " +
                "  \"data\": " + gmv +
                "}";
    }

    @RequestMapping("api/sugar/ch")
    public String getUvByCh(@RequestParam(value = "date", defaultValue = "0") int date,
                            @RequestParam(value = "limit", defaultValue = "5") int limit) {

        if (date == 0) {
            date = getToday();
        }

        //查询数据
        Map uvByCh = uvService.getUvByCh(date, limit);

        //取出Key和Values
        Set chs = uvByCh.keySet();
        Collection uvs = uvByCh.values();

        //拼接字符串并返回
        return "{" +
                "  \"status\": 0," +
                "  \"msg\": \"\"," +
                "  \"data\": {" +
                "    \"categories\": [\"" +
                StringUtils.join(chs, "\",\"") +
                "\"]," +
                "    \"series\": [" +
                "      {" +
                "        \"name\": \"日活\"," +
                "        \"data\": [" +
                StringUtils.join(uvs, ",") +
                "]" +
                "      }" +
                "    ]" +
                "  }" +
                "}";
    }

    private int getToday() {
        long ts = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return Integer.parseInt(sdf.format(ts));
    }


}
