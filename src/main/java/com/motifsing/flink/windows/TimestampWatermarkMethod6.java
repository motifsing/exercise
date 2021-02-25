package com.motifsing.flink.windows;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Iterator;

/**
 * @ClassName TimestampWatermarkMethod2
 * @Description
 * @Author Motifsing
 * @Date 2021/2/24 10:53
 * @Version 1.0
 **/
public class TimestampWatermarkMethod6 {

    private static final OutputTag<String> late = new OutputTag<String>("late", BasicTypeInfo.STRING_TYPE_INFO){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<String> stringDataStreamSource = env.addSource(new SourceFunction<String>() {

            private Boolean isCancel = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int num = 1;
                while (isCancel) {
                    long currentTimeMillis = System.currentTimeMillis();
                    if (num % 2 == 0) {
                        currentTimeMillis -= 4000;
                    }
                    String testStr = currentTimeMillis + "\tMotifsing\t" + num;
                    System.out.println("source: " + testStr);
                    num++;
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

        SingleOutputStreamOperator<String> process = stringSingleOutputStreamOperator.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                String[] split = value.split("\t");
                return split[1];
            }
        }).timeWindow(Time.seconds(2))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(late)
                .process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        System.out.println("subtask:" + getRuntimeContext().getIndexOfThisSubtask() +
                                ",start:" + context.window().getStart() +
                                ",end:" + context.window().getEnd() +
                                ",waterMarks:" + context.currentWatermark() +
                                ",currentTime:" + System.currentTimeMillis());

                        Iterator<String> iterator = elements.iterator();
                        for (; iterator.hasNext(); ) {
                            String next = iterator.next();
                            System.out.println("windows-->" + next);
                            out.collect("on time:" + next);
                        }
                    }
                });

        // 处理准时数据
        process.print();

        // 处理延时数据
        DataStream<String> lateOutput = process.getSideOutput(late);
        lateOutput.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return "late:" + value;
            }
        }).print();

        env.execute();
    }
}
