package com.motifsing.flink.windows;

import akka.stream.impl.ReducerState;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName ConnectJoinTimeServerStateSessionWindow
 * @Description
 * @Author Motifsing
 * @Date 2021/3/2 9:39
 * @Version 1.0
 **/
public class ConnectJoinTimeServerStateSessionWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> s1 = env.socketTextStream("localhost", 8888);
        DataStreamSource<String> s2 = env.socketTextStream("localhost", 9999);

        KeyedStream<Tuple2<String, Integer>, String> input1 = s1.map(f -> Tuple2.of(f, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(f -> f.f0);
        KeyedStream<Tuple2<String, Integer>, String> input2 = s2.map(f -> Tuple2.of(f, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(f -> f.f0);

        ConnectedStreams<Tuple2<String, Integer>, Tuple2<String, Integer>> connect = input1.connect(input2);
        
        connect.process(new KeyedCoProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
            private ValueState<Long> dateTime = null;
            private final int interval = 3000;
            private ReducingState<String> outString = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ReducingStateDescriptor<String> rsd = new ReducingStateDescriptor<>("outStr", new ReduceFunction<String>() {
                    @Override
                    public String reduce(String value1, String value2) throws Exception {
                        return value1 + "\t" + value2;
                    }
                }, String.class);

                outString = getRuntimeContext().getReducingState(rsd);

                ValueStateDescriptor<Long> vsd = new ValueStateDescriptor<>("dateTime", Long.class);
                dateTime = getRuntimeContext().getState(vsd);
            }

            @Override
            public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                outString.add(value.f0);
                long l = System.currentTimeMillis();
                dateTime.update(l);
                ctx.timerService().registerProcessingTimeTimer(l + interval);
                System.out.println("sub_task_id_1:" + getRuntimeContext().getIndexOfThisSubtask() + ",value:" + value.f0);
            }

            @Override
            public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                outString.add(value.f0);
                long l = System.currentTimeMillis();
                dateTime.update(l);
                ctx.timerService().registerProcessingTimeTimer(l + interval);
                System.out.println("sub_task_id_2:" + getRuntimeContext().getIndexOfThisSubtask() + ",value:" + value.f0);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                System.out.println("timeServer start!");
                if (timestamp == dateTime.value() + interval){
                    out.collect(outString.get());
                    outString.clear();
                }
            }
        }).print();

        env.execute();
    }
}
