package com.motifsing.flink.windows;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @ClassName ReduceFunctionOnCountWindowAll
 * @Description
 * @Author Motifsing
 * @Date 2021/2/26 10:31
 * @Version 1.0
 **/
public class ReduceFunctionOnCountWindowAll {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        ArrayList<Tuple2<String, Long>> list = new ArrayList<>();
        list.add(Tuple2.of("Motifsing", 1L));
        list.add(Tuple2.of("Motifsing2", 2L));
        list.add(Tuple2.of("Motifsing", 3L));
        list.add(Tuple2.of("Motifsing2", 4L));
        list.add(Tuple2.of("Motifsing3", 100L));

        DataStreamSource<Tuple2<String, Long>> input = env.fromCollection(list);

        SingleOutputStreamOperator<Tuple2<String, Long>> reduce = input.countWindowAll(2).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });

        reduce.print();

        env.execute();
    }
}
