package com.motifsing.flink.operator;

import com.motifsing.flink.source.FileCountryDictSourceMapFunction;
import com.motifsing.flink.source.MyKafkaRecord;
import com.motifsing.flink.source.MyKafkaRecordSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @ClassName CountryCodeConnect
 * @Description
 * @Author Motifsing
 * @Date 2021/1/13 17:03
 * @Version 1.0
 **/
public class CountryCodeConnectMap {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        String path = "/user/test/countryCode/test.txt";
        DataStreamSource<Map<String, String>> countryCodeSource = env.addSource(new FileCountryDictSourceMapFunction(path));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.23.44.249:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("flink.partition-discovery.interval-millis", "30000");

        String topic = "test";

        FlinkKafkaConsumer<MyKafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new MyKafkaRecordSchema(), properties);
        kafkaConsumer.setStartFromLatest();

        DataStreamSource<MyKafkaRecord> kafkaInput = env.addSource(kafkaConsumer);

        ConnectedStreams<Map<String, String>, MyKafkaRecord> connect = countryCodeSource.connect(kafkaInput);

        SingleOutputStreamOperator<String> process = connect.process(new CoProcessFunction<Map<String, String>, MyKafkaRecord, String>() {

            HashMap<String, String> hashMap = new HashMap<>();

            @Override
            public void processElement1(Map<String, String> value, Context ctx, Collector<String> out) throws Exception {
                for (Map.Entry<String, String> entry: value.entrySet()){
                    hashMap.put(entry.getKey(), entry.getValue());
                }
                out.collect(value.toString());
            }

            @Override
            public void processElement2(MyKafkaRecord value, Context ctx, Collector<String> out) throws Exception {
                String countryName = hashMap.get(value.getRecord());
                String outStr = countryName == null ? "no match" : countryName;
                out.collect(value + ": " + outStr);
            }
        });

        process.print();

        env.execute();
    }
}
