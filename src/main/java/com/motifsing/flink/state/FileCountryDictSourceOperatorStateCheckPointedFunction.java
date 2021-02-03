package com.motifsing.flink.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @ClassName FileCountryDictSourceFunction
 * @Description
 * @Author Motifsing
 * @Date 2021/1/8 16:41
 * @Version 1.0
 **/
public class FileCountryDictSourceOperatorStateCheckPointedFunction implements SourceFunction<String>, CheckpointedFunction {

    private Boolean isCancel = false;

    private Integer interval = 10000;

    private String md5 = null;

    private String path;

    private ListState<String> ls = null;

    public FileCountryDictSourceOperatorStateCheckPointedFunction(String path, Integer interval){
        this.path = path;
        this.interval = interval;
    }

    public FileCountryDictSourceOperatorStateCheckPointedFunction(String path){
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
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        ls.clear();
        ls.add(md5);
        System.out.println("snapshotState");
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<String> lsd = new ListStateDescriptor<>("md5", String.class);
        ls = context.getOperatorStateStore().getListState(lsd);
        if (context.isRestored()) {
            Iterable<String> strings = ls.get();
            String next = strings.iterator().next();
            md5 = next;
        }
    }
}
