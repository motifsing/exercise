package com.motifsing.flink.state;

import com.motifsing.flink.source.FileCountryDictSourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @ClassName FileSourceRestart
 * @Description 失败重试
 * @Author Motifsing
 * @Date 2021/2/2 14:35
 * @Version 1.0
 **/
public class FileSourceRestartMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.每 1000ms 开始一次 checkpoint
        env.enableCheckpointing(1000);

        // 高级选项：
        // 设置模式为精确一次 (这是默认值)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 确认 checkpoints 之间的时间会进行 500 ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // Checkpoint 必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 开启在 job 中止后仍然保留的 externalized checkpoints
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 2.设置重试策略
        // Time.of(0, TimeUnit.SECONDS) 不能设置0？
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(6, Time.of(1, TimeUnit.SECONDS)));

        // 3.checkpoint存储位置
        FsStateBackend fsStateBackend = new FsStateBackend("file:///C:\\Users\\Administrator\\Desktop\\checkpoint", true);
        env.setStateBackend(fsStateBackend);

        String path = "/user/test/countryCode/test.txt";
//        DataStreamSource<String> source = env.addSource(new FileCountryDictSourceFunction(path));
//        DataStreamSource<String> source = env.addSource(new FileCountryDictSourceOperatorStateCheckPointedFunction(path));
        DataStreamSource<String> source = env.addSource(new FileCountryDictSourceOperatorStateListCheckPointedFunction(path));

        source.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println(value);
                if (value.contains("中国")) {
                    int a = 1/0;
                }
                return value;
            }
        }).uid("map").name("This is a map...").print();

        env.execute();
    }
}
