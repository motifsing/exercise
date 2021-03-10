package com.motifsing.flink.windows;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author Motifsing
 * @Date 2021/03/01 22:52
 */
public class ConnectJoinTimeServerSessionWindow {
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
            private Long dateTime = null;
            private final int interval = 3000;
            private String outString = "";

            @Override
            public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                outString += value.f0 + "\t";
                long l = System.currentTimeMillis();
                dateTime = l;
                ctx.timerService().registerProcessingTimeTimer(l + interval);
                System.out.println("subTaskId1:" + getRuntimeContext().getIndexOfThisSubtask() + ",value:" + value.f0);
            }

            @Override
            public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                outString += value.f0 + "\t";
                long l = System.currentTimeMillis();
                dateTime = l;
                ctx.timerService().registerProcessingTimeTimer(l + interval);
                System.out.println("subTaskId2:" + getRuntimeContext().getIndexOfThisSubtask() + ",value:" + value.f0);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                System.out.println("TimeServer start!");
                if (timestamp == dateTime + interval){
                    out.collect(outString);
                    outString = "";
                }
            }
        }).print();

        env.execute();
    }
}
