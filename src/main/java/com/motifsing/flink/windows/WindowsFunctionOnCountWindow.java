package com.motifsing.flink.windows;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @ClassName GlobalWindowsH
 * @Description
 * @Author Motifsing
 * @Date 2021/2/25 16:35
 * @Version 1.0
 **/
public class WindowsFunctionOnCountWindow {
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

        SingleOutputStreamOperator<Tuple2<String, Long>> aggregate = keyBy.window(GlobalWindows.create())
                .trigger(CountTrigger.of(5))
                .evictor(new Evictor<Tuple3<String, Long, Long>, GlobalWindow>() {
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<Tuple3<String, Long, Long>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
                        for (Iterator<TimestampedValue<Tuple3<String, Long, Long>>> iterator = elements.iterator(); iterator.hasNext(); ) {
                            TimestampedValue<Tuple3<String, Long, Long>> next = iterator.next();
                            System.out.println("Before:" + next.getValue());
                            if (next.getValue().f2 % 5 == 0) {
                                iterator.remove();
                            }
                        }
                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<Tuple3<String, Long, Long>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
                        for (TimestampedValue<Tuple3<String, Long, Long>> next : elements) {
                            System.out.println("After:" + next.getValue());
                        }
                    }
                })
                .aggregate(new AggregateFunction<Tuple3<String, Long, Long>, Tuple2<String, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        return Tuple2.of("", 0L);
                    }

                    @Override
                    public Tuple2<String, Long> add(Tuple3<String, Long, Long> value, Tuple2<String, Long> accumulator) {
                        return Tuple2.of(value.f0, value.f2 + accumulator.f1);
                    }

                    @Override
                    public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
                        return Tuple2.of(a.f0, a.f1 + b.f1);
                    }
                });

        aggregate.print();

        env.execute();
    }
}
