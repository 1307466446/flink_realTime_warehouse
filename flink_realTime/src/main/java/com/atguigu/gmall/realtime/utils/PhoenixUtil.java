package com.atguigu.gmall.realtime.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Blue红红
 * @description 基于JDBC连接用来查询数据的返回是一个list, 查询的数据可能是多行
 * @create 2022/6/26 16:33
 */
public class PhoenixUtil {

    public static <T> List<T> query(Connection connection, String sql, Class<T> clz, boolean isUnderScoreToCamel) throws Exception {
        ArrayList<T> list = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            list = new ArrayList<>();
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();

            // 获得元数据
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                T t = clz.newInstance();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = resultSet.getObject(columnName);

                    if (isUnderScoreToCamel) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                    }
                    BeanUtils.setProperty(t, columnName, value);
                }
                list.add(t);
            }
            return list;

        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
