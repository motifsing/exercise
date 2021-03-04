package com.motifsing.flink.statistics.top;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.text.SimpleDateFormat;

/**
 * @ClassName RandomSourceFunction
 * @Description
 * @Author Motifsing
 * @Date 2021/3/4 17:00
 * @Version 1.0
 **/
public class ActivitySourceFunction implements SourceFunction<Tuple3<ActivityEnum, String, Long>> {

    private Boolean isCancel = true;

    private long lateTimeMillis;

    public ActivitySourceFunction() {
    }

    public ActivitySourceFunction(long lateTimeMillis) {
        this.lateTimeMillis = lateTimeMillis;
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        while (isCancel){
            long currentTimeMillis = System.currentTimeMillis() - (int) (Math.random()*lateTimeMillis);
            String format = "yyyy-MM-dd  HH:mm:ss";
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            String dateTimeStr = sdf.format(currentTimeMillis);
            ActivityEnum activityEnum = ActivityEnum.randomType(ActivityEnum.values());
//            System.out.println("source: " + activityEnum.getName() + ", " + dateTimeStr);
            ctx.collect(Tuple3.of(activityEnum, dateTimeStr, currentTimeMillis));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isCancel = false;
    }
}
