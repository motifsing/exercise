package com.motifsing.flink.windows;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @ClassName TumblingWindows
 * @Description
 * @Author Motifsing
 * @Date 2021/2/25 11:21
 * @Version 1.0
 **/
public class TumblingWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stringDataStreamSource = env.addSource(new SourceFunction<String>() {

            private Boolean isCancel = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int num = 1;
                while (isCancel) {
                    long currentTimeMillis = System.currentTimeMillis();
                    String testStr = currentTimeMillis + "\tMotifsing\t" + num;
//                    num++;
                    ctx.collect(testStr);
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isCancel = false;
            }
        });

        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = stringDataStreamSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String element) {
                String[] split = element.split("\t");
                return Long.parseLong(split[0]);
            }
        });

        KeyedStream<String, String> keyedStream = stringSingleOutputStreamOperator.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                String[] split = value.split("\t");
                return split[1];
            }
        });

        keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(3)))
                .process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        int sum = 0;
                        System.out.println("subtask:" + getRuntimeContext().getIndexOfThisSubtask() +
                                ",start:" + context.window().getStart() +
                                ",end:" + context.window().getEnd() +
                                ",waterMarks:" + context.currentWatermark() +
                                ",currentTime:" + System.currentTimeMillis());
                        Iterator<String> iterator = elements.iterator();
                        for (;iterator.hasNext();){
                            String next = iterator.next();
                            System.out.println("next: " + next);
                            String[] split = next.split("\t");
                            int i = Integer.parseInt(split[2]);
                            sum += i;
                        }
                        out.collect("sum: " + sum);
                    }
                }).print();

        env.execute();
    }
}
