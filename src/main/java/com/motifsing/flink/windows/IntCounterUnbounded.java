package com.motifsing.flink.windows;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName IntCounterBounded
 * @Description
 * @Author Motifsing
 * @Date 2021/3/2 15:16
 * @Version 1.0
 **/
public class IntCounterUnbounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        DataStreamSource<String> input = env.socketTextStream("localhost", 9999);

        input.flatMap(new RichFlatMapFunction<String, Tuple3<String, Integer, IntCounter>>() {
            private IntCounter inc = null;
            @Override
            public void open(Configuration parameters) throws Exception {
                inc = new IntCounter();
                getRuntimeContext().addAccumulator("Motifsing", inc);
            }

            @Override
            public void flatMap(String value, Collector<Tuple3<String, Integer, IntCounter>> out) throws Exception {
                String[] strs = value.split(" ");
                for (String str:strs){
                    inc.add(1);
                    out.collect(Tuple3.of(str, 1, inc));
                }
            }
        }).print();

        env.execute();

    }
}
