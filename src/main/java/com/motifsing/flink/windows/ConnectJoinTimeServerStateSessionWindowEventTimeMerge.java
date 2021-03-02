package com.motifsing.flink.windows;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;

/**
 * @ClassName ConnectJoinTimeServerStateSessionWindow
 * @Description
 * @Author Motifsing
 * @Date 2021/3/2 9:39
 * @Version 1.0
 **/
public class ConnectJoinTimeServerStateSessionWindowEventTimeMerge {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> s1 = env.socketTextStream("localhost", 8888);
        DataStreamSource<String> s2 = env.socketTextStream("localhost", 9999);

        KeyedStream<Tuple3<String, Integer, Long>, Tuple> input1 = s1.map(f -> Tuple3.of(f, 1, System.currentTimeMillis()))
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG))
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Long>>() {
                    @NotNull
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(System.currentTimeMillis());
                    }

                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Long> element, long previousElementTimestamp) {
                        return element.f2;
                    }
                })
                .keyBy("f0");
        KeyedStream<Tuple3<String, Integer, Long>, Tuple> input2 = s2.map(f -> Tuple3.of(f, 1, System.currentTimeMillis()))
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG))
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Long>>() {
                    @NotNull
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(System.currentTimeMillis());
                    }

                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Long> element, long previousElementTimestamp) {
                        return element.f2;
                    }
                })
                .keyBy("f0");

        ConnectedStreams<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>> connect = input1.connect(input2);
        
        connect.process(new KeyedCoProcessFunction<String, Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, String>() {
            private ValueState<Long> dateTime = null;
            private final int interval = 3000;
            private ReducingState<String> outString = null;
            private ValueState<Long> lastTime = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ReducingStateDescriptor<String> rsd = new ReducingStateDescriptor<>("outStr", new ReduceFunction<String>() {
                    @Override
                    public String reduce(String value1, String value2) throws Exception {
                        return value1 + "\t" + value2;
                    }
                }, String.class);

                outString = getRuntimeContext().getReducingState(rsd);

                ValueStateDescriptor<Long> vsd1 = new ValueStateDescriptor<>("dateTime", Long.class);
                dateTime = getRuntimeContext().getState(vsd1);

                ValueStateDescriptor<Long> vsd2 = new ValueStateDescriptor<>("lastTime", Long.class);
                lastTime = getRuntimeContext().getState(vsd2);
            }

            public void doWork(Tuple3<String, Integer, Long> value, Context ctx, String taskName) throws Exception {
                outString.add(value.f0);
                if (lastTime.value() != null){
                    ctx.timerService().deleteEventTimeTimer(lastTime.value());
                }
                long l = value.f2;
                dateTime.update(l);
                lastTime.update(l + interval);
                ctx.timerService().registerEventTimeTimer(lastTime.value());
                System.out.println(taskName + getRuntimeContext().getIndexOfThisSubtask() + ", value:" + value.f0);
            }

            @Override
            public void processElement1(Tuple3<String, Integer, Long> value, Context ctx, Collector<String> out) throws Exception {
                doWork(value, ctx, "sub_task_id_1:");
            }

            @Override
            public void processElement2(Tuple3<String, Integer, Long> value, Context ctx, Collector<String> out) throws Exception {
                doWork(value, ctx, "sub_task_id_2:");
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                System.out.println("timeServer start!");
                if (timestamp == dateTime.value() + interval){
                    out.collect(outString.get());
                    outString.clear();
                    lastTime.clear();
                }
            }
        }).print();

        env.execute();
    }
}
