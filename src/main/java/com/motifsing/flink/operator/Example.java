package com.motifsing.flink.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @ClassName Example
 * @Description
 * @Author Administrator
 * @Date 2020/12/9 17:02
 * @Version 1.0
 **/
public class Example {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> flintstones = env.fromElements(
                new Person("Fred", 35),
                new Person("Wilma", 35),
                new Person("Pebbles", 2));

        DataStream<Person> adults1 = flintstones.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person person) {
                return person.age >= 18;
            }
        });
        DataStream<Person> adults2 = flintstones.filter((FilterFunction<Person>) person -> person.age >= 18);

        DataStream<Person> adults = flintstones.filter((person) -> person.age >= 18);

        adults.print();

        env.execute();
    }

    public static class Person {
        public String name;
        public Integer age;

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return this.name + ": age " + this.age.toString();
        }
    }
}
