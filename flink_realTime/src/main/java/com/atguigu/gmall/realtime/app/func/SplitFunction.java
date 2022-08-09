package com.atguigu.gmall.realtime.app.func;

import com.atguigu.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * @author Blue红红
 * @description 功能描述
 * @create 2022/6/21 19:49
 */

// 自定义函数实现一进多出功能
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {
    
    public void eval(String keyword) {
        List<String> list;
        try {
            list = KeywordUtil.splitKeyword(keyword);
            for (String word : list) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
            collect(Row.of(keyword));
        }
    }
    
}
