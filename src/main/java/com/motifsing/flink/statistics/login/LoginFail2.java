package com.motifsing.flink.statistics.login;

import com.motifsing.flink.statistics.login.beans.LoginEvent;
import com.motifsing.flink.statistics.login.beans.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.Iterator;

/**
 * @ClassName LoginFail
 * @Description
 * @Author Motifsing
 * @Date 2021/3/30 19:38
 * @Version 1.0
 **/
public class LoginFail2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取数据
        URL resource = LoginFail2.class.getResource("/LoginLog.csv");
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
                .process(new LoginFailDetectWarning(2));

        warningStream.print();

        env.execute("login fail detect warning job");
    }

    /**
     * 实现自定义keyedProcessFunction
     */
    public static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {

        // 定义属性，时间长度
        private Integer timeWindow;

        public LoginFailDetectWarning(Integer timeWindow) {
            this.timeWindow = timeWindow;
        }

        // 定义状态：保存登录失败事件
        ListState<LoginEvent> loginFailEventListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<>("login-fail-state", LoginEvent.class));
        }

        // 以登录事件状态作为报警的判断条件，不再注册定时器
        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            // 判断登录状态
            if ("fail".equals(value.getLoginState())) {
                // 1. 如果登录失败，获取状态中的登录事件，判断状态中是否有登录失败事件
                Iterator<LoginEvent> iterator = loginFailEventListState.get().iterator();
                if (iterator.hasNext()) {
                    // 1.1 如果状态有登录事件，判断间隔时间是否小于设定时间
                    Long firstLoginFailTime = iterator.next().getTimestamp();
                    if (value.getTimestamp() - firstLoginFailTime <= timeWindow) {
                        // 如果时间小于设定2s，则发射数据
                        out.collect(new LoginFailWarning(value.getUserId(), firstLoginFailTime, value.getTimestamp(),
                                "login failed 2 times in " + timeWindow + "s"));
                    }
                    // 不管是否报警，本次事件已处理完毕，更新状态
                    loginFailEventListState.clear();
                    loginFailEventListState.add(value);

                } else {
                    // 1.2 如果状态中没有登录失败事件，则添加到状态中
                    loginFailEventListState.add(value);
                }
            } else {
                // 2. 如果成功，则清空状态
                loginFailEventListState.clear();
            }
        }
    }
}
