package com.motifsing.flink.state;

import com.motifsing.flink.source.FileCountryDictSourceMapFunction;
import com.motifsing.flink.source.MyKafkaRecord;
import com.motifsing.flink.source.MyKafkaRecordSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName CountryCodeConnect
 * @Description
 * @Author Motifsing
 * @Date 2021/1/13 17:03
 * @Version 1.0
 **/
public class CountryCodeConnectMapBroadCast {

    private static final MapStateDescriptor<String, String> BROADCAST1 = new MapStateDescriptor<>("broadcast", String.class, String.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 1.每 1000ms 开始一次 checkpoint
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.设置重试策略
        // Time.of(0, TimeUnit.SECONDS) 不能设置0？
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(6, Time.of(1, TimeUnit.SECONDS)));

        String path = "/user/test/countryCode/test.txt";
        DataStreamSource<Map<String, String>> countryCodeSource = env.addSource(new FileCountryDictSourceMapFunction(path));
        BroadcastStream<Map<String, String>> broadcast = countryCodeSource.broadcast(BROADCAST1);
        

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.23.44.249:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("flink.partition-discovery.interval-millis", "30000");

        String topic = "test";

        FlinkKafkaConsumer<MyKafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new MyKafkaRecordSchema(), properties);
        kafkaConsumer.setStartFromLatest();

        DataStreamSource<MyKafkaRecord> kafkaInput = env.addSource(kafkaConsumer);

        BroadcastConnectedStream<MyKafkaRecord, Map<String, String>> connect = kafkaInput.connect(broadcast);

        SingleOutputStreamOperator<String> process = connect.process(new BroadcastProcessFunction<MyKafkaRecord, Map<String, String>, String>() {
            @Override
            public void processElement(MyKafkaRecord value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(BROADCAST1);
                String countryCode = value.getRecord();
                String countryName = broadcastState.get(countryCode);
                String outStr = countryName == null ? "no match" : countryName;
                out.collect(value + ": " + outStr);
            }

            @Override
            public void processBroadcastElement(Map<String, String> value, Context ctx, Collector<String> out) throws Exception {
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(BROADCAST1);
                for (Map.Entry<String, String> entry: value.entrySet()){
                    broadcastState.put(entry.getKey(), entry.getValue());
                }
                out.collect(value.toString());
            }
        });

        process.print();

        env.execute();
    }
}
