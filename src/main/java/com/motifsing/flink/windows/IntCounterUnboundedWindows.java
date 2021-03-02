package com.motifsing.flink.windows;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @ClassName IntCounterBounded
 * @Description
 * @Author Motifsing
 * @Date 2021/3/2 15:16
 * @Version 1.0
 **/
public class IntCounterUnboundedWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        DataStreamSource<String> input = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Integer> reduce = input.flatMap(new RichFlatMapFunction<String, Integer>() {
            @Override
            public void flatMap(String value, Collector<Integer> out) throws Exception {
                String[] strings = value.split(" ");
                for (String str : strings) {
                    out.collect(1);
                }
            }
        }).timeWindowAll(Time.seconds(5))
                .reduce(new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                        return value1 + value2;
                    }
                });

        Iterator<Integer> collect = DataStreamUtils.collect(reduce);

        for (;collect.hasNext();){
            Integer next = collect.next();
            // 插入数据库
            System.out.println("count:" + next);
        }

        env.execute();

    }
}
