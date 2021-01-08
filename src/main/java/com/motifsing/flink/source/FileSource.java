package com.motifsing.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName FileSource
 * @Description
 * @Author Motifsing
 * @Date 2021/1/8 17:29
 * @Version 1.0
 **/
public class FileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String path = "/user/test/test.txt";
//        String path = "file:///C:\\Users\\Administrator\\Desktop\\test.txt";

        DataStreamSource<String> stringDataStreamSource = env.addSource(new FileCountryDictSourceFunction(path));

        stringDataStreamSource.print();

        env.execute();
    }
}
