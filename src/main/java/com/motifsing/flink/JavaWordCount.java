package com.motifsing.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;


/**
 * @author 12284
 * @Date 2020/1/6 22:00
 */
public class JavaWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> socket = env.socketTextStream("localhost", 9999, "\n");

//        // 第一种方法, lambda表达式
//        SingleOutputStreamOperator<String> flatMap = socket.flatMap(
//                (String value, Collector<String> out) -> Arrays.stream(value.split(" ")
//                ).forEach(out::collect))
//                .returns(Types.STRING);
//
//        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatMap.map(f -> Tuple2.of(f, 1))
//                .returns(Types.TUPLE(Types.STRING, Types.INT));
//
//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = map.keyBy(0).sum(1);
//
//        sum.print();

//        // 第二种写法，function
//        SingleOutputStreamOperator<String> flatMap = socket.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String value, Collector<String> out) throws Exception {
//                String[] s = value.split(" ");
//                for (String ss : s) {
//                    out.collect(ss);
//                }
//            }
//        });
//
//        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
//
//            @Override
//            public Tuple2<String, Integer> map(String value) throws Exception {
//                return Tuple2.of(value, 1);
//            }
//        });
//
//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = map.keyBy("f0").sum("f1");
//
//        sum.print();

        env.execute("JavaWordCount");
    }
}
