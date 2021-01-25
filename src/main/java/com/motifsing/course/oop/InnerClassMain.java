package com.motifsing.course.oop;

import java.util.HashMap;

/**
 * @ClassName InnerClassMain
 * @Description 内部类
 * @Author Motifsing
 * @Date 2021/1/25 17:35
 * @Version 1.0
 **/
public class InnerClassMain {
    public static void main(String[] args) {
        // 实例化一个Outer
        Outer outer = new Outer("Outer");
        // 实例化一个Inner
        Outer.Inner inner = outer.new Inner();
        inner.run();

        // 匿名类
        outer.asyncHello();

        // 匿名类（匿名类也完全可以继承自普通类）观察编译输出可发现Main$1.class和Main$2.class两个匿名类文件。
        // map1普通的HashMap实例
        HashMap<String, String> map1 = new HashMap<>();
        // map2是一个匿名类实例，只是该匿名类继承自HashMap
        HashMap<String, String> map2 = new HashMap<String, String>() {};
        // map3也是一个继承自HashMap的匿名类实例，并且添加了static代码块来初始化数据
        HashMap<String, String> map3 = new HashMap<String, String>() {
            {
                put("A", "1");
                put("B", "2");
            }
        };
        System.out.println(map3.get("A"));

        // 静态内部类
        Outer.StaticNested staticNested = new Outer.StaticNested();
        staticNested.hello();

    }
}

/**
 * Java的内部类可分为Inner Class、Anonymous Class和Static Nested Class三种：
 * Inner Class和Anonymous Class本质上是相同的，都必须依附于Outer Class的实例，即隐含地持有Outer.this实例，并拥有Outer Class的private访问权限；
 * Static Nested Class是独立类，但拥有Outer Class的private访问权限。
 */
class Outer {

    private String name;

    private static String NAME = "OUTER";

    Outer(String name){
        this.name = name;
    }

    /**
     * 1. Inner Class除了有一个this指向它自己，还隐含地持有一个Outer Class实例，可以用Outer.this访问这个实例。
     *  所以，实例化一个Inner Class不能脱离Outer实例
     * 2. Inner Class和普通Class相比，除了能引用Outer实例外，还有一个额外的“特权”，就是可以修改Outer Class的private字段，
     *  因为Inner Class的作用域在Outer Class内部，所以能访问Outer Class的private字段和方法。
     * 3. 观察Java编译器编译后的.class文件可以发现，Outer类被编译为Outer.class，而Inner类被编译为Outer$Inner.class
     */
    class Inner {

        public void run() {
            System.out.println("Inner Class:" + Outer.this.name);
        }
    }

    /**
     * Anonymous Class(匿名类)
     * 我们在方法内部实例化了一个Runnable。
     * Runnable本身是接口，接口是不能实例化的，所以这里实际上是定义了一个实现了Runnable接口的匿名类，并且通过new实例化该匿名类，然后转型为Runnable
     * 匿名类和Inner Class一样，可以访问Outer Class的private字段和方法。之所以我们要定义匿名类，是因为在这里我们通常不关心类名，比直接定义Inner Class可以少写很多代码
     * 观察Java编译器编译后的.class文件可以发现，Outer类被编译为Outer.class，而匿名类被编译为Outer$1.class。
     * 如果有多个匿名类，Java编译器会将每个匿名类依次命名为Outer$1、Outer$2、Outer$3……
     */
    void asyncHello() {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                System.out.println("Hello, " + Outer.this.name);
            }
        };
        new Thread(r).start();
    }

    /**
     * Static Nested Class(静态内部类)
     * 用static修饰的内部类和Inner Class有很大的不同，它不再依附于Outer的实例，而是一个完全独立的类，因此无法引用Outer.this，
     * 但它可以访问Outer的private静态字段和静态方法。如果把StaticNested移到Outer之外，就失去了访问private的权限。
     */
    static class StaticNested {
        void hello() {
            System.out.println("Hello, " + Outer.NAME);
        }
    }

}