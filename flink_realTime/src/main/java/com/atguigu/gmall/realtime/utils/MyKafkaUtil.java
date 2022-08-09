package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

/**
 * @author Blue红红
 * @description
 * @create 2022/6/13 23:57
 */
public class MyKafkaUtil {

    private static final String BOOTSTRAP_SERVERS = "hadoop102:9092";

    public static FlinkKafkaConsumer<String> getFlinkConsumer(String topic, String groupId) {

        Properties properties = new Properties(); 
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new FlinkKafkaConsumer<>(topic, new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String nextElement) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                if (record == null || record.value() == null) {
                    return "";
                } else {
                    return new String(record.value());
                }
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return BasicTypeInfo.STRING_TYPE_INFO;
            }
        }, properties);
    }


    public static FlinkKafkaProducer<String> getFlinkProducer(String topic) {
        return new FlinkKafkaProducer<>(BOOTSTRAP_SERVERS, topic, new SimpleStringSchema());
    }


    /**
     * 将kafka的数据读取到对应的表中
     *
     * @param topic
     * @param groupId
     * @return
     */
    public static String getKafkaDDL(String topic, String groupId) {
        return " with ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'group-offsets')";
    }

    /**
     * 将表的数据写入到kafka中
     *
     * @param topic
     * @return
     */
    public static String getKafkaSinkDDL(String topic) {
        return " with ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
                " 'format' = 'json' " +
                ")";
    }


    /**
     * 对于leftJoin，fullJoin，RightJoin来说会有撤回流，我们用upsert-kafka，
     *
     * @param topic
     * @return
     */
    public static String getUpsertKafkaDDL(String topic) {
        return "WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }

    /**
     * 因为这张topic_db表是一张常用表
     * 后面的事务表都需要从这张表中过滤属于自己的数据
     * @param groupId
     * @return
     */
    public static String getTopic_Db_DDL(String groupId) {
        return " create table topic_db( " +
                " `database` String, " +
                " `table` String, " +
                " `type` String, " +
                " `ts` bigint, " +
                " `xid` bigint, " +
                " `xoffset` bigint," +
                " `data` Map<String,String>," +  // 因为是json格式我们用map
                " `old` Map<String,String>, " +
                " `pt` AS PROCTIME() " +  // 处理时间
                " ) " + getKafkaDDL("topic_db", groupId);
    }


}
