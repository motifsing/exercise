package com.motifsing.flink.operator;

import com.motifsing.flink.source.FileCountryDictSourceFunction;
import com.motifsing.flink.source.MyKafkaRecord;
import com.motifsing.flink.source.MyKafkaRecordSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

/**
 * @ClassName CountryCodeKeyBy
 * @Description
 * @Author Motifsing
 * @Date 2021/1/13 17:19
 * @Version 1.0
 **/
public class CountryCodeConnectKeyBy {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        String path = "";
        DataStreamSource<String> countryCodeSource = env.addSource(new FileCountryDictSourceFunction(path));

        KeyedStream<Tuple2<String, String>, String> countryCodeKeyedStream = countryCodeSource.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] s = value.split(" ");
                return Tuple2.of(s[0], s[1] + "-" + s[2]);
            }
        }).keyBy(new KeySelector<Tuple2<String, String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> value) throws Exception {
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

        KeyedStream<MyKafkaRecord, String> kafkaKeyedStream = kafkaInput.keyBy(new KeySelector<MyKafkaRecord, String>() {
            @Override
            public String getKey(MyKafkaRecord value) throws Exception {
                return value.getRecord();
            }
        });

        ConnectedStreams<Tuple2<String, String>, MyKafkaRecord> connect = countryCodeKeyedStream.connect(kafkaKeyedStream);

        SingleOutputStreamOperator<String> process = connect.process(new KeyedCoProcessFunction<String, Tuple2<String, String>, MyKafkaRecord, String>() {

            HashMap<String, String> hashMap = new HashMap<>();

            @Override
            public void processElement1(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
                hashMap.put(value.f0, value.f1);
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
