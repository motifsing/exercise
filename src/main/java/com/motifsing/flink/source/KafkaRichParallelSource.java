package com.motifsing.flink.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author Motifsing
 */
public class KafkaRichParallelSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", ":9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("flink.partition-discovery.interval-millis", "30000");

        String topic = "test";

        FlinkKafkaConsumer<MyKafkaRecord> kafkaSource = new FlinkKafkaConsumer<>(topic, new MyKafkaRecordSchema(), properties);

        env.addSource(kafkaSource).print();

        env.execute();

    }
}
