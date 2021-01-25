package com.motifsing.course.oop;

/**
 * @ClassName StaticMain
 * @Description 静态方法和静态字段
 * @Author Motifsing
 * @Date 2021/1/25 15:49
 * @Version 1.0
 **/
public class StaticMain {
    public static void main(String[] args) {
        // 推荐用类名来访问静态字段。可以把静态字段理解为描述class本身的字段（非实例字段）
        // 通过实例变量也可以调用静态字段和方法，但这只是编译器自动帮我们把实例改写成类名而已。
        Company c1 = new Company("a");
        Company.country = "China";
        System.out.println(Company.country);
        Company.run();

        Round r = new Round();
        System.out.println(r.pi);
        System.out.println(Round.name);
    }
}

class Company {
    public String name;
    /**
     * 静态字段
     */
    public static String country;

    public Company(String name){
        this.name = name;
    }

    /**
     * 静态方法
     * 因为静态方法属于class而不属于实例，因此，静态方法内部，无法访问this变量，也无法访问实例字段，它只能访问静态字段。
     */

    public static void run(){
        System.out.println("I'm method of static...");
    }
}

/**
 * 因为interface是一个纯抽象类，所以它不能定义实例字段。但是，interface是可以有静态字段的，并且静态字段必须为final类型
 * 因为interface的字段只能是public static final类型，所以我们可以把这些修饰符都去掉
 */
interface Circle {
    public static final double pi = 3.1415926;
    String name = "circle";
}

class Round implements Circle {
    double pi =  Circle.pi;
}