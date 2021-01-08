package com.motifsing.flink.source;

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
public class FileCountryDictSourceFunction implements SourceFunction<String> {

    private Boolean isCancel = false;

    private Integer interval = 10000;

    private String md5 = null;

    private String path;

    public FileCountryDictSourceFunction(String path, Integer interval){
        this.path = path;
        this.interval = interval;
    }

    public FileCountryDictSourceFunction(String path){
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
}
