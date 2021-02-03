package com.motifsing.flink.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName FileCountryDictSourceFunction
 * @Description
 * @Author Motifsing
 * @Date 2021/1/8 16:41
 * @Version 1.0
 **/
public class FileCountryDictSourceOperatorStateListCheckPointedFunction implements SourceFunction<String>, ListCheckpointed<String> {

    private Boolean isCancel = false;

    private Integer interval = 10000;

    private String md5 = null;

    private String path;


    public FileCountryDictSourceOperatorStateListCheckPointedFunction(String path, Integer interval){
        this.path = path;
        this.interval = interval;
    }

    public FileCountryDictSourceOperatorStateListCheckPointedFunction(String path){
        this.path = path;
    }


    @Override
    public void run(SourceContext ctx) throws Exception {
        Path path = new Path(this.path);
        FileSystem fs = FileSystem.get(new Configuration());
        while (!isCancel) {
            if(!fs.exists(path)){
                Thread.sleep(interval);
                continue;
            }
            System.out.println(md5);
            FileChecksum fileChecksum = fs.getFileChecksum(path);
            String md5Str = fileChecksum.toString();
            String currentMd5 = md5Str.substring(md5Str.indexOf(":") + 1);

            if (!currentMd5.equals(md5)){
                FSDataInputStream open = fs.open(path);
                BufferedReader reader = new BufferedReader(new InputStreamReader(open));
                String line = reader.readLine();
                while (line != null){
                    ctx.collect(line);
                    line = reader.readLine();
                }
                reader.close();
                md5 = currentMd5;

            }
            Thread.sleep(interval);
        }
    }

    @Override
    public void cancel() {
        isCancel = true;
        System.out.println("cancel");
    }

    @Override
    public List<String> snapshotState(long checkpointId, long timestamp) throws Exception {
        ArrayList<String> list = new ArrayList<>();
        list.add(md5);
        System.out.println("snapshotState");
        return list;
    }

    @Override
    public void restoreState(List<String> state) throws Exception {
        md5 = state.get(0);
    }
}





