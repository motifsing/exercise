package com.motifsing.flink.state;

import com.motifsing.flink.source.FileCountryDictSourceFunction;
import com.motifsing.flink.source.MyKafkaRecord;
import com.motifsing.flink.source.MyKafkaRecordSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName CountryCodeKeyBy
 * @Description
 * @Author Motifsing
 * @Date 2021/1/13 17:19
 * @Version 1.0
 **/
public class CountryCodeConnectKeyByKeyedState {
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
//        DataStreamSource<String> countryCodeSource = env.addSource(new FileCountryDictSourceFunction(path));
        DataStreamSource<String> countryCodeSource = env.addSource(new FileCountryDictSourceOperatorStateListCheckPointedFunction(path));

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
        properties.setProperty("bootstrap.servers", "172.23.44.249:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("flink.partition-discovery.interval-millis", "30000");

        String topic = "test";

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

            private MapState<String, String> ms = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = StateTtlConfig
                        .newBuilder(Time.seconds(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp) // 状态可见性
                        .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
                        .build();

                MapStateDescriptor<String, String> msd = new MapStateDescriptor<>("map", String.class, String.class);
                msd.enableTimeToLive(ttlConfig);
                ms = getRuntimeContext().getMapState(msd);
            }

            @Override
            public void processElement1(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
                ms.put(value.f0, value.f1);
                out.collect(value.toString());
            }

            @Override
            public void processElement2(MyKafkaRecord value, Context ctx, Collector<String> out) throws Exception {

                for (Map.Entry<String, String> entry:ms.entries()) {
                    System.out.println("key: " + entry.getKey() + ", value: " + entry.getValue());
                }
                if ("CN".equals(value.getRecord())) {
                    int i = 1/0;
                }
                String countryName = ms.get(value.getRecord());
                String outStr = countryName == null ? "no match" : countryName;
                out.collect(value + ": " + outStr);
            }
        });

        process.print();

        env.execute();
    }
}
