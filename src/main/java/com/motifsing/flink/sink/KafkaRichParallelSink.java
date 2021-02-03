package com.motifsing.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @ClassName KafkaRichParallelSink
 * @Author Motifsing
 * @Date 2021/2/2 11:02
 * @Version 1.0
 **/
public class KafkaRichParallelSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);

        String topic = "test";

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "172.23.44.249:9092");
        properties.setProperty("retries", "3");

        FlinkKafkaProducer<String> stringFlinkKafkaProducer = new FlinkKafkaProducer<>(topic, new SimpleStringSchema(), properties);

        socketTextStream.addSink(stringFlinkKafkaProducer);

        env.execute();

    }
}
