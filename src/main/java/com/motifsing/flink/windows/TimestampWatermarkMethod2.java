package com.motifsing.flink.windows;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;

/**
 * @ClassName TimestampWatermarkMethod2
 * @Description
 * @Author Motifsing
 * @Date 2021/2/24 10:53
 * @Version 1.0
 **/
public class TimestampWatermarkMethod2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stringDataStreamSource = env.addSource(new SourceFunction<String>() {

            private Boolean isCancel = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isCancel) {
                    long currentTimeMillis = System.currentTimeMillis();
                    String testStr = currentTimeMillis + "\tMotifsing";
                    ctx.collect(testStr);
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isCancel = false;
            }
        });

        stringDataStreamSource.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {

            private long watermarkTime;

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(watermarkTime);
            }

            @Override
            public long extractTimestamp(String element, long previousElementTimestamp) {
                String[] split = element.split("\t");
                long eventTime = Long.parseLong(split[0]);
                watermarkTime = eventTime - 1000;
                return eventTime;
            }
        }).print();

        env.execute();
    }
}
