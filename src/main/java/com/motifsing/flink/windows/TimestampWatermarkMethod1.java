package com.motifsing.flink.windows;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * @ClassName TimestampWatermarkMethod1
 * @Description
 * @Author Motifsing
 * @Date 2021/2/24 10:31
 * @Version 1.0
 **/
public class TimestampWatermarkMethod1 {

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

                    String[] split = testStr.split("\t");
                    long timeStamp = Long.parseLong(split[0]);
                    String data = split[1];

                    ctx.collectWithTimestamp(data, timeStamp);
                    ctx.emitWatermark(new Watermark(currentTimeMillis - 1000));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isCancel = false;
            }
        });

        stringDataStreamSource.print();

        env.execute();
    }
}
