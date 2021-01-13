package com.motifsing.flink.operator;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.Arrays;


/**
 * @author 12284
 * @Date 2020/1/6 22:00
 */
public class SocketWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // 全局禁用operator chaining
        // 减少线程切换， 减少序列化和反序列化，减少数据在缓冲区的交换， 减少延迟并且提高吞吐力
//        env.disableOperatorChaining();

        DataStreamSource<String> socket = env.socketTextStream("localhost", 9999, "\n");

        // 第一种方法, lambda表达式
        SingleOutputStreamOperator<String> flatMap = socket.flatMap(
                (String value, Collector<String> out) -> Arrays.stream(value.split(" ")
                ).forEach(out::collect))
                .returns(Types.STRING);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatMap.map(f -> Tuple2.of(f, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT)).setParallelism(2);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = map.keyBy(0).sum(1).setParallelism(2);

        // 改变并行度也可以禁用operator chaining
//        sum.print().setParallelism(3);
        // 禁用operator chaining(NEVER)
//        sum.print().disableChaining().setParallelism(2);
        // 设置分组名称，名称不同，也可以禁用 operator chaining, 默认都是default组
//        sum.print().slotSharingGroup("Motifsing").setParallelism(2);

        // 只与下游合并，不与上游合并
        sum.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) throws Exception {
                return true;
            }
        }).startNewChain().setParallelism(2).print().setParallelism(2);


        System.out.println(env.getExecutionPlan());

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

//        // 第三种写法，function组合写法
//        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = socket.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//
//            @Override
//            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                String[] s = value.split(" ");
//                for (String ss : s) {
//                    out.collect(Tuple2.of(ss, 1));
//                }
//            }
//        });
//
//        // lambda写法
//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = flatMap.keyBy(f -> f.f0).sum("f1");
//
//        sum.print();

//        // 第四种写法，richFunction
//        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = socket.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
//
//            private String name = null;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
////                super.open(parameters);
//                name = "Motifsing";
//            }
//
//            @Override
//            public void close() throws Exception {
////                super.close();
//                name = null;
//            }
//
//            @Override
//            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
//                String[] s = value.split(" ");
//                for (String ss : s) {
//                    out.collect(Tuple2.of(name + "_" + ss + "_" + indexOfThisSubtask, 1));
//                }
//            }
//        });
//
//        // function
//        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = flatMap.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
//            @Override
//            public String getKey(Tuple2<String, Integer> value) throws Exception {
//                return value.f0;
//            }
//        });
//
//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tuple2StringKeyedStream.sum(1);
//
//        sum.print();

//        // 第五种写法 procee + function 不健壮，没有容错机制
//
//        KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = socket.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
//
//            private String name = null;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
////                super.open(parameters);
//                name = "Motifsing";
//            }
//
//            @Override
//            public void close() throws Exception {
////                super.close();
//                name = null;
//            }
//
//            @Override
//            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
//                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
//                String[] s = value.split(" ");
//                for (String ss : s) {
//                    System.out.println(indexOfThisSubtask);
//                    out.collect(Tuple2.of(name + "_" + ss, 1));
//                }
//            }
//        }).keyBy(0);
//
//        // function
//        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = tuple2TupleKeyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
//                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
//            }
//        });
//
//        SingleOutputStreamOperator<Tuple2<String, Integer>> process = tuple2TupleKeyedStream.process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
//
//            private Integer num = 0;
//
//            @Override
//            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
//                num += value.f1;
//                out.collect(Tuple2.of(value.f0, num));
//            }
//        });
//
//        process.print();
//
//        reduce.print();


        env.execute("JavaWordCount");
    }
}
