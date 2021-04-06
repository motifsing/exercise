package com.motifsing.flink.statistics.login;

import com.motifsing.flink.statistics.login.beans.LoginEvent;
import com.motifsing.flink.statistics.login.beans.LoginFailWarning;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * @ClassName LoginFailWithCep
 * @Description
 * @Author Motifsing
 * @Date 2021/3/31 15:22
 * @Version 1.0
 **/
public class LoginFailWithCep {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取数据
        URL resource = LoginFailWithCep.class.getResource("/LoginLog.csv");
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

        // 1. 定义一个匹配模式
        // firstFail -> secondFail, within 2s
//        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>begin("firstFail").where(new SimpleCondition<LoginEvent>() {
//            @Override
//            public boolean filter(LoginEvent value) throws Exception {
//                return "fail".equals(value.getLoginState());
//            }
//        }).next("secondFail").where(new SimpleCondition<LoginEvent>() {
//            @Override
//            public boolean filter(LoginEvent value) throws Exception {
//                return "fail".equals(value.getLoginState());
//            }
//        }).within(Time.seconds(2));
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>begin("firstFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getLoginState());
                    }
                })
                .times(3).consecutive()
                .within(Time.seconds(5));

        // 2. 将匹配模式应用到数据流上，得到一个pattern stream
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream.keyBy(LoginEvent::getUserId), loginFailPattern);

        // 3. 检出符合匹配条件的复杂事件，进行转换处理，得到报警信息
        SingleOutputStreamOperator<LoginFailWarning> warningStream = patternStream.select(new LoginFailMatchDetectWarning());

        warningStream.print();

        env.execute("login fail detect with cep job");
    }

    /**
     * 实现自定义的PatternSelectFunction
     */
    public static class LoginFailMatchDetectWarning implements PatternSelectFunction<LoginEvent, LoginFailWarning>{

        @Override
        public LoginFailWarning select(Map<String, List<LoginEvent>> pattern) throws Exception {
//            LoginEvent firstFail = pattern.get("firstFail").iterator().next();
//            LoginEvent secondFail = pattern.get("secondFail").get(0);
//            return new LoginFailWarning(firstFail.getUserId(), firstFail.getTimestamp(), secondFail.getTimestamp(),
//                    "login failed 2 times in 2s");
            LoginEvent firstFail = pattern.get("firstFail").get(0);
            LoginEvent lastFail = pattern.get("firstFail").get(pattern.get("firstFail").size() - 1);
            return new LoginFailWarning(firstFail.getUserId(), firstFail.getTimestamp(), lastFail.getTimestamp(),
                    "login failed " + pattern.get("firstFail").size() + " times in 5s");
        }
    }
}
