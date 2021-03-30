package com.motifsing.flink.statistics.login;

import com.clearspring.analytics.util.Lists;
import com.motifsing.flink.statistics.login.beans.LoginEvent;
import com.motifsing.flink.statistics.login.beans.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.List;

/**
 * @ClassName LoginFail
 * @Description
 * @Author Motifsing
 * @Date 2021/3/30 19:38
 * @Version 1.0
 **/
public class LoginFail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取数据
        URL resource = LoginFail.class.getResource("/LoginLog.csv");
        SingleOutputStreamOperator<LoginEvent> loginEventStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000;
                    }
                });

        // 自定义处理函数检测连续登录失败事件
        SingleOutputStreamOperator<LoginFailWarning> warningStream = loginEventStream
                .keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectWarning(2, 2));

        warningStream.print();

        env.execute("login fail detect warning job");
    }

    /**
     * 实现自定义keyedProcessFunction
     */
    public static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {

        // 定义属性，最大连续登录失败次数
        private Integer maxFailTimes;
        // 定义属性，定时器时间
        private Integer timeWindow;

        public LoginFailDetectWarning(Integer maxFailTimes, Integer timeWindow) {
            this.maxFailTimes = maxFailTimes;
            this.timeWindow = timeWindow;
        }

        // 定义状态：保存2s内所有的登录失败事件
        ListState<LoginEvent> loginFailEventListState;

        // 定义状态：保存注册的定时器时间戳
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<>("login-fail-state", LoginEvent.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<>("timer-ts-state", Long.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            // 判断当前登录事件类型
            if ("fail".equals(value.getLoginState())){
                // 1. 如果是登录失败，添加到列表状态中
                loginFailEventListState.add(value);
                // 如果没有定时器，注册一个2s之后的定时器
                if (timerTsState.value() == null){
                    long ts = (value.getTimestamp() + timeWindow) * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    timerTsState.update(ts);
                }
            } else if ("success".equals(value.getLoginState())) {
                // 2. 如果是登录成功，删除定时器，清空状态，重新开始
                if (timerTsState.value() != null) {
                    ctx.timerService().deleteEventTimeTimer(timerTsState.value());
                }
                loginFailEventListState.clear();
                timerTsState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {
            // 定时器触发，说明2s之内没有登录成功事件，需要判断loginFailEventListState中登录失败次数
            List<LoginEvent> loginFailEvents = Lists.newArrayList(loginFailEventListState.get());
            int failTimes = loginFailEvents.size();

            if (failTimes >= maxFailTimes) {
                out.collect(new LoginFailWarning(ctx.getCurrentKey(),
                        loginFailEvents.get(0).getTimestamp(),
                        loginFailEvents.get(failTimes - 1).getTimestamp(),
                        "login failed " + failTimes + " times in 2s"));
            }

            // 清空状态
            loginFailEventListState.clear();
            timerTsState.clear();

        }
    }
}
