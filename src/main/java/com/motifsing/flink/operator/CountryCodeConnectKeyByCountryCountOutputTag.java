package com.motifsing.flink.operator;

import com.motifsing.flink.source.FileCountryDictSourceFunction;
import com.motifsing.flink.source.MyKafkaRecord;
import com.motifsing.flink.source.MyKafkaRecordSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Properties;

/***
 * @author Motifsing
 * @ClassName CountryCodeConnectKeyByCountryCountOutputTag
 * @Description
 * @Date 2021/1/19 14:52
 * @Version 1.0
 */

public class CountryCodeConnectKeyByCountryCountOutputTag {

    private static final OutputTag<String> ot = new OutputTag<String>("AM"){};

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        String path = "/user/test/test.txt";
        DataStreamSource<String> countryCodeSource = env.addSource(new FileCountryDictSourceFunction(path));

        KeyedStream<Tuple2<MyKafkaRecord, String>, MyKafkaRecord> countryCodeKeyedStream = countryCodeSource.map(new MapFunction<String, Tuple2<MyKafkaRecord, String>>() {
            @Override
            public Tuple2<MyKafkaRecord, String> map(String value) throws Exception {
                String[] s = value.split(" ");
                return Tuple2.of(new MyKafkaRecord(new String(s[0])), s[1] + "-" + s[2]);
            }
        }).keyBy(new KeySelector<Tuple2<MyKafkaRecord, String>, MyKafkaRecord>() {
            @Override
            public MyKafkaRecord getKey(Tuple2<MyKafkaRecord, String> value) throws Exception {
                return value.f0;
            }
        });

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.23.36.74:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("flink.partition-discovery.interval-millis", "30000");

        String topic = "test";

        FlinkKafkaConsumer<MyKafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new MyKafkaRecordSchema(), properties);
        kafkaConsumer.setStartFromLatest();

        DataStreamSource<MyKafkaRecord> kafkaInput = env.addSource(kafkaConsumer);

        KeyedStream<MyKafkaRecord, MyKafkaRecord> kafkaKeyedStream = kafkaInput.keyBy(new KeySelector<MyKafkaRecord, MyKafkaRecord>() {
            @Override
            public MyKafkaRecord getKey(MyKafkaRecord value) throws Exception {
                return value;
            }
        });

        ConnectedStreams<Tuple2<MyKafkaRecord, String>, MyKafkaRecord> connect = countryCodeKeyedStream.connect(kafkaKeyedStream);

        SingleOutputStreamOperator<Tuple2<String, Integer>> process = connect.process(new KeyedCoProcessFunction<MyKafkaRecord, Tuple2<MyKafkaRecord, String>, MyKafkaRecord, Tuple2<String, Integer>>() {
            HashMap<String, String> hashMap = new HashMap<>();

            @Override
            public void processElement1(Tuple2<MyKafkaRecord, String> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                hashMap.put(value.f0.getRecord(), value.f1);
                out.collect(Tuple2.of(value.f0.getRecord(), 3));
            }

            @Override
            public void processElement2(MyKafkaRecord value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                String countryName = hashMap.get(value.getRecord());
                String outStr = countryName == null ? "no match" : countryName;
                if (value.getRecord().contains("AM")) {
                    ctx.output(ot, outStr);
                }
                out.collect(Tuple2.of(value.getRecord(), 2));
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> outPut = process.keyBy(0).process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            Integer minValue = Integer.MAX_VALUE;

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                if (value.f1 < minValue) {
                    out.collect(value);
                    minValue = value.f1;
                } else {
                    out.collect(Tuple2.of(value.f0, minValue));
                }
            }
        });

        outPut.print();

        DataStream<String> sideOutput = process.getSideOutput(ot);
//        sideOutput.print();

//        String targetPath = "C:\\Users\\Administrator\\Desktop\\test";
//        sideOutput.writeAsText(targetPath, FileSystem.WriteMode.OVERWRITE);

        sideOutput.writeToSocket("localhost", 9999, new SimpleStringSchema());

        env.execute();
    }
}
