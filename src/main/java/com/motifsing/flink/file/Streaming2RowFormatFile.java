package com.motifsing.flink.file;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

import java.time.ZoneId;

/**
 * @ClassName Streaming2RowFormatFile
 * @Description
 * @Author Motifsing
 * @Date 2021/3/2 16:38
 * @Version 1.0
 **/
public class Streaming2RowFormatFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);


        BucketingSink<String> bucketingSink = new BucketingSink("file:///C:\\Users\\Administrator\\Desktop\\test");

        bucketingSink.setBucketer(new DateTimeBucketer("yyyy-MM-dd-HH", ZoneId.of("Asia/Shanghai")));
        bucketingSink.setBatchRolloverInterval(10000);
        bucketingSink.setBatchSize(1024 * 1024 * 10);
        bucketingSink.setPendingPrefix("Motifsing");
        bucketingSink.setPendingSuffix(".txt");
        bucketingSink.setInProgressPrefix(".");

        socketTextStream.addSink(bucketingSink);

        env.execute();
    }
}
