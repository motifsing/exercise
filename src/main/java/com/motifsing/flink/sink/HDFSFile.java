package com.motifsing.flink.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName HDFSFile
 * @Author Motifsing
 * @Date 2021/2/1 16:52
 * @Version 1.0
 **/
public class HDFSFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);

        socketTextStream.addSink(new HDFSSinkFunction());

        env.execute();
    }
}
