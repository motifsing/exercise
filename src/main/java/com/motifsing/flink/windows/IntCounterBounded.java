package com.motifsing.flink.windows;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
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
public class IntCounterBounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> input = env.fromElements("a b c", "d e f");

        input.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            private IntCounter inc = null;
            @Override
            public void open(Configuration parameters) throws Exception {
                inc = new IntCounter();
                getRuntimeContext().addAccumulator("Motifsing", inc);
            }

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] strs = value.split(" ");
                for (String str:strs){
                    inc.add(1);
                    out.collect(Tuple2.of(str, 1));
                }
            }
        }).print();

        JobExecutionResult execute = env.execute();

        Integer motifsing = execute.getAccumulatorResult("Motifsing");
        System.out.println(motifsing);
    }
}
