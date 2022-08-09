package com.atguigu.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Blue红红
 * @description 功能描述
 * @create 2022/6/21 19:50
 */
public class KeywordUtil {
    
    public static List<String> splitKeyword(String keyWord) throws IOException {
        
        // 用于存储切分后的单词
        ArrayList<String> resultList = new ArrayList<>();

        StringReader reader = new StringReader(keyWord);
        IKSegmenter ikSegmenter = new IKSegmenter(reader,false); // false:表示maxWord切词

        Lexeme next = ikSegmenter.next();
        
        while (next != null){
            String lexemeText = next.getLexemeText();
            resultList.add(lexemeText);
            next=ikSegmenter.next() ;
        }

        return resultList;
    }
}
