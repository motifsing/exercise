package com.motifsing.flink.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @ClassName HDFSSinkFunction
 * @Description 自定义SinkFunction
 * @Author Motifsing
 * @Date 2021/2/1 15:32
 * @Version 1.0
 **/
public class HDFSSinkFunction extends RichSinkFunction<String> {

    private FileSystem fs;
    private String pathStr;
    private SimpleDateFormat sf;

    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        fs = FileSystem.get(hadoopConf);
        sf = new SimpleDateFormat("yyyyMMddHH");
        pathStr = "/user/test/sink";
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        if (value != null){
            String format = sf.format(new Date());
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            StringBuilder sb = new StringBuilder();
            sb.append(pathStr).append("/").append(indexOfThisSubtask).append("_").append(format);

            Path path = new Path(sb.toString());
            FSDataOutputStream fsd;
            if (fs.exists(path)) {
                fsd = fs.append(path);
            } else {
                fsd = fs.create(path);
            }

            fsd.write((value + "\n").getBytes(StandardCharsets.UTF_8));
            fsd.close();
        }
    }
}











