package com.motifsing.flink.operator;

import com.motifsing.flink.source.FileCountryDictSourceFunction;
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
import java.util.Properties;

/**
 * @ClassName CountryCodeConnect
 * @Description
 * @Author Motifsing
 * @Date 2021/1/13 17:03
 * @Version 1.0
 **/
public class CountryCodeConnect {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        String path = "/user/test/test.txt";
        DataStreamSource<String> countryCodeSource = env.addSource(new FileCountryDictSourceFunction(path));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.23.36.74:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("flink.partition-discovery.interval-millis", "30000");

        String topic = "test";

        FlinkKafkaConsumer<MyKafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new MyKafkaRecordSchema(), properties);
        kafkaConsumer.setStartFromLatest();

        DataStreamSource<MyKafkaRecord> kafkaInput = env.addSource(kafkaConsumer);

        ConnectedStreams<String, MyKafkaRecord> connect = countryCodeSource.connect(kafkaInput);

        SingleOutputStreamOperator<String> process = connect.process(new CoProcessFunction<String, MyKafkaRecord, String>() {

            HashMap<String, String> hashMap = new HashMap<>();

            @Override
            public void processElement1(String value, Context ctx, Collector<String> out) throws Exception {
                String[] s = value.split(" ");
                hashMap.put(s[0], s[1] + "-" + s[2]);
                out.collect(value);
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
