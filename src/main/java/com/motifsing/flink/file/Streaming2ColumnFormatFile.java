package com.motifsing.flink.file;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName Streaming2ColumnFormatFile
 * @Description
 * @Author Motifsing
 * @Date 2021/3/2 17:34
 * @Version 1.0
 **/
public class Streaming2ColumnFormatFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        env.enableCheckpointing(1000);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // 保存EXACTLY_ONCE
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 每次ck之间的间隔，不会重叠
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
        // 每次ck的超时时间
        checkpointConfig.setCheckpointTimeout(2000L);
        // 如果ck执行失败，程序是否停止
        checkpointConfig.setFailOnCheckpointingErrors(true);
//        checkpointConfig.setTolerableCheckpointFailureNumber(0);
        // job在执行CANCE的时候是否删除ck数据
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 指定保存ck的存储模式，这个是默认的
        MemoryStateBackend memoryStateBackend = new MemoryStateBackend(10 * 1024 * 1024, false);
        env.setStateBackend(memoryStateBackend);

        // 恢复策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(0, TimeUnit.SECONDS)));

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<MotifsingParquetPojo> map = socketTextStream.map(f -> Tuple2.of(f, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(0)
                .sum(1)
                .map(f -> new MotifsingParquetPojo(f.f0, f.f1));

        DateTimeBucketAssigner<MotifsingParquetPojo> objectDateTimeBucketAssigner = new DateTimeBucketAssigner<>("yyyy/MMdd/HH", ZoneId.of("Asia/Shanghai"));

        StreamingFileSink<MotifsingParquetPojo> build = StreamingFileSink
                .forBulkFormat(new Path("file:///C:\\Users\\Administrator\\Desktop\\parquet"),
                        ParquetAvroWriters.forReflectRecord(MotifsingParquetPojo.class))
                .withBucketAssigner(objectDateTimeBucketAssigner)
                .build();

        map.addSink(build);

        env.execute();


    }
}
