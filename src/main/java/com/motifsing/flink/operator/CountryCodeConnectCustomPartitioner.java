package com.motifsing.flink.operator;

import com.motifsing.flink.source.FileCountryDictSourceFunction;
import com.motifsing.flink.source.MyKafkaRecord;
import com.motifsing.flink.source.MyKafkaRecordSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

/**
 * @ClassName CountryCodeKeyBy
 * @Description
 * @Author Motifsing
 * @Date 2021/1/13 23:40
 * @Version 1.0
 **/
public class CountryCodeConnectCustomPartitioner {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        String path = "";
        DataStreamSource<String> countryCodeSource = env.addSource(new FileCountryDictSourceFunction(path));

        DataStream<Tuple2<MyKafkaRecord, String>> countryCodeDataStream = countryCodeSource
                .flatMap(new FlatMapFunction<String, Tuple2<MyKafkaRecord, String>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<MyKafkaRecord, String>> out) throws Exception {
                        String[] split = value.split(" ");
                        String key = split[0];
                        String values = split[1] + "-" + split[2];
                        for (int i=0; i<4; i++){
                            String randomKey = i + "_" + key;
                            Tuple2<MyKafkaRecord, String> t2 = Tuple2.of(new MyKafkaRecord(randomKey), values);
                            out.collect(t2);
                        }
                    }
                }).partitionCustom(new MyPartitioner(), new KeySelector<Tuple2<MyKafkaRecord, String>, MyKafkaRecord>() {
                    @Override
                    public MyKafkaRecord getKey(Tuple2<MyKafkaRecord, String> value) throws Exception {
                        return value.f0;
                    }
                });

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "");
        properties.setProperty("group.id", "");
        properties.setProperty("flink.partition-discovery.interval-millis", "30000");

        String topic = "";

        FlinkKafkaConsumer<MyKafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new MyKafkaRecordSchema(), properties);
        kafkaConsumer.setStartFromLatest();

        DataStreamSource<MyKafkaRecord> kafkaInput = env.addSource(kafkaConsumer);

        DataStream<MyKafkaRecord> myKafkaDataStream = kafkaInput.map(new MapFunction<MyKafkaRecord, MyKafkaRecord>() {
            @Override
            public MyKafkaRecord map(MyKafkaRecord value) throws Exception {
                String record = value.getRecord();
                Random random = new Random();
                int i = random.nextInt(4);
                return new MyKafkaRecord(i + "_" + record);
            }
        }).partitionCustom(new MyPartitioner(), new KeySelector<MyKafkaRecord, MyKafkaRecord>() {
            @Override
            public MyKafkaRecord getKey(MyKafkaRecord value) throws Exception {
                return value;
            }
        });

        ConnectedStreams<Tuple2<MyKafkaRecord, String>, MyKafkaRecord> connect = countryCodeDataStream.connect(myKafkaDataStream);

        SingleOutputStreamOperator<String> process = connect.process(new CoProcessFunction<Tuple2<MyKafkaRecord, String>, MyKafkaRecord, String>() {

            HashMap<String, String> hashMap = new HashMap<>();

            @Override
            public void processElement1(Tuple2<MyKafkaRecord, String> value, Context ctx, Collector<String> out) throws Exception {
                hashMap.put(value.f0.getRecord(), value.f1);
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
