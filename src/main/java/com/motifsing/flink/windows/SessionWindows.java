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
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @ClassName SessionWindows
 * @Description
 * @Author Motifsing
 * @Date 2021/2/25 15:54
 * @Version 1.0
 **/
public class SessionWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple3<String, Long, Long>> streamSource = env.addSource(new SourceFunction<Tuple3<String, Long, Long>>() {

            private Boolean isCancel = true;

            @Override
            public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
                long num = 0L;
                while (isCancel) {
                    num++;
                    ctx.collect(Tuple3.of("Motifsing", System.currentTimeMillis(), num));
                    if (num % 5 == 0) {
                        Thread.sleep(3000);
                    } else {
                        Thread.sleep(1000);
                    }
                }
            }

            @Override
            public void cancel() {
                isCancel = false;
            }
        });

        SingleOutputStreamOperator<Tuple3<String, Long, Long>> tuple3SingleOutputStreamOperator = streamSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Long, Long>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<String, Long, Long> element) {
                return element.f1;
            }
        });

        KeyedStream<Tuple3<String, Long, Long>, String> keyBy = tuple3SingleOutputStreamOperator.keyBy(new KeySelector<Tuple3<String, Long, Long>, String>() {
            @Override
            public String getKey(Tuple3<String, Long, Long> value) throws Exception {
                return value.f0;
            }
        });

//        SingleOutputStreamOperator<Tuple3<String, Long, Long>> reduce = keyBy.window(EventTimeSessionWindows.withGap(Time.seconds(2))).reduce(new ReduceFunction<Tuple3<String, Long, Long>>() {
//            @Override
//            public Tuple3<String, Long, Long> reduce(Tuple3<String, Long, Long> value1, Tuple3<String, Long, Long> value2) throws Exception {
//                return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
//            }
//        });
//
//        reduce.print();

//        SingleOutputStreamOperator<Tuple3<String, Long, Long>> process = keyBy.window(EventTimeSessionWindows.withGap(Time.seconds(2))).process(new ProcessWindowFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>, String, TimeWindow>() {
//            @Override
//            public void process(String s, Context context, Iterable<Tuple3<String, Long, Long>> elements, Collector<Tuple3<String, Long, Long>> out) throws Exception {
//                for (Tuple3<String, Long, Long> next : elements) {
//                    out.collect(next);
//                }
//            }
//        });

        SingleOutputStreamOperator<Tuple3<String, Long, Long>> process = keyBy.window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple3<String, Long, Long>>() {
            @Override
            public long extract(Tuple3<String, Long, Long> element) {
                if (element.f2 % 5 == 0) {
                    return 2500L;
                } else {
                    return 2000L;
                }
            }
        })).process(new ProcessWindowFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<Tuple3<String, Long, Long>> elements, Collector<Tuple3<String, Long, Long>> out) throws Exception {
                for (Tuple3<String, Long, Long> next : elements) {
                    out.collect(next);
                }
            }
        });

        process.print();

        env.execute();
    }
}
