package com.motifsing.flink.windows;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;


/**
 * @ClassName SlidingWindows
 * @Description
 * @Author Motifsing
 * @Date 2021/2/25 14:21
 * @Version 1.0
 **/
public class SlidingWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple3<String, Long, Long>> stringDataStreamSource = env.addSource(new SourceFunction<Tuple3<String, Long, Long>>() {

            private Boolean isCancel = true;

            @Override
            public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
                Long num = 1L;
                while (isCancel) {
                    ctx.collect(Tuple3.of("Motifsing", System.currentTimeMillis(), num));
                    Thread.sleep(1000);
                    num++;
                }
            }

            @Override
            public void cancel() {
                isCancel = false;
            }
        });

        SingleOutputStreamOperator<Tuple3<String, Long, Long>> stringSingleOutputStreamOperator = stringDataStreamSource.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<Tuple3<String, Long, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple3<String, Long, Long> element) {
                        return element.f1;
                    }
                }
        );

        KeyedStream<Tuple3<String, Long, Long>, String> keyBy = stringSingleOutputStreamOperator.keyBy(new KeySelector<Tuple3<String, Long, Long>, String>() {
            @Override
            public String getKey(Tuple3<String, Long, Long> value) throws Exception {
                return value.f0;
            }
        });

//        SingleOutputStreamOperator<Tuple3<String, Long, Long>> reduce = keyBy.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))).reduce(new ReduceFunction<Tuple3<String, Long, Long>>() {
//            @Override
//            public Tuple3<String, Long, Long> reduce(Tuple3<String, Long, Long> value1, Tuple3<String, Long, Long> value2) throws Exception {
//                return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
//            }
//        });
//
//        reduce.print();

        SingleOutputStreamOperator<Tuple3<String, Long, Long>> process = keyBy.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))).process(
                new ProcessWindowFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>, String, TimeWindow>() {

                    @Override
                    public void process(String s, Context context, Iterable<Tuple3<String, Long, Long>> elements, Collector<Tuple3<String, Long, Long>> out) throws Exception {
                        System.out.println("subtask:" + getRuntimeContext().getIndexOfThisSubtask() +
                                ",start:" + context.window().getStart() +
                                ",end:" + context.window().getEnd() +
                                ",waterMarks:" + context.currentWatermark() +
                                ",currentTime:" + System.currentTimeMillis());
                        Iterator<Tuple3<String, Long, Long>> iterator = elements.iterator();
                        for (; iterator.hasNext(); ) {
                            Tuple3<String, Long, Long> next = iterator.next();
                            out.collect(next);
                        }
                    }
                });

        process.print();

        env.execute();
    }
}
