package com.motifsing.flink.operator;

import com.motifsing.flink.source.FileCountryDictSourceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

/**
 * @ClassName CountryCodeUnion
 * @Description
 * @Author Motifsing
 * @Date 2021/1/13 15:52
 * @Version 1.0
 **/
public class CountryCodeUnion {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        String path = "";
        DataStreamSource<String> countryCodeSource = env.addSource(new FileCountryDictSourceFunction(path));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "");
        properties.setProperty("group.id", "");
        properties.setProperty("flink.partition-discovery.interval-millis", "30000");

        String topic = "test";

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromLatest();

        DataStreamSource<String> kafkaInput = env.addSource(kafkaConsumer);

        DataStream<String> union = countryCodeSource.union(kafkaInput);

        SingleOutputStreamOperator<String> process = union.process(new ProcessFunction<String, String>() {

            HashMap<String, String> hashMap = new HashMap<>();

            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                String[] s = value.split(" ");
                if (s.length > 1) {
                    hashMap.put(s[0], s[1] + "-" + s[2]);
                    out.collect(value);
                } else {
                    String countryName = hashMap.get(value);
                    String outStr = countryName == null ? "no match" : countryName;
                    out.collect(outStr);
                }
            }
        });

        process.print();

        env.execute();
    }
}
