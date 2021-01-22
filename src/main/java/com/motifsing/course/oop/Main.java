package com.motifsing.course.oop;

import java.util.Arrays;

/**
 * @ClassName Method
 * @Description 执行入口
 * @Author Motifsing
 * @Date 2021/1/21 11:26
 * @Version 1.0
 **/
public class Main {
    public static void main(String[] args) {
        Person person = new Person();
        person.setName("Motifsing");
        person.setBirth(20);
        System.out.println("name: " + person.getName() + "\n" + "age: " + person.getBirth());
        person.setNames("a");
        System.out.println(Arrays.toString(person.getNames()));
        person.setNames("a", "b");
        System.out.println(Arrays.toString(person.getNames()));

        // 基本类型参数绑定（基本类型参数的传递，是调用方值的复制。双方各自的后续修改，互不影响）
        int birth = 2000;
        person.setBirth(birth);
        System.out.println(person.getBirth());
        birth = 2010;
        System.out.println(person.getBirth());

        // 引用类型参数绑定(引用类型参数的传递，调用方的变量，和接收方的参数变量，指向的是同一个对象)
        // 双方任意一方对这个对象的修改，都会影响对方（因为指向同一个对象嘛）
        String[] fullname = new String[] { "Homer", "Simpson" };
        person.setNames(fullname);
        System.out.println(Arrays.toString(person.getNames()));
        fullname[0] = "Bart";
        System.out.println(Arrays.toString(person.getNames()));

        // 同样是引用类型，为什么name修改后，实例person的name没有发生变化（字符串不可变，只是name的引用指向变了，Bob是没有变的）
        String name = "Bob";
        person.setName(name);
        System.out.println(person.getName());
        name = "Jack";
        System.out.println(person.getName());
    }
}
