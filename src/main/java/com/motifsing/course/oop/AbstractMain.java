package com.motifsing.course.oop;

/**
 * @ClassName AbstractMain
 * @Description 抽象类
 * @Author Motifsing
 * @Date 2021/1/25 14:41
 * @Version 1.0
 **/
public class AbstractMain {
    /**
     * 这种尽量引用高层类型，避免引用实际子类型的方式，称之为面向抽象编程
     * 面向抽象编程的本质就是：
     * 1. 上层代码只定义规范（例如：abstract class Animals）
     * 2. 不需要子类就可以实现业务逻辑（正常编译）；
     * 3. 具体的业务逻辑由不同的子类实现，调用者并不关心。
     */
    public static void main(String[] args) {
        // 当我们定义了抽象类Animals，以及具体的Cat、Dog子类的时候，我们可以通过抽象类Animals类型去引用具体的子类的实例
        Animals a1 = new Cat();
        Animals a2 = new Dog();

        // 这种引用抽象类的好处在于，我们对其进行方法调用，并不关心Animals类型变量的具体子类型
        a1.run();
        a2.run();

        // 同样的代码，如果引用的是一个新的子类，我们仍然不关心具体类型
        Animals a3 = new Panda();
        a3.run();
    }
}

abstract class Animals{

    /**
     * 如果父类的方法本身不需要实现任何功能，仅仅是为了定义方法签名，目的是让子类去覆写它，那么，可以把父类的方法声明为抽象方法
     * 如果一个class定义了方法，但没有具体执行代码，这个方法就是抽象方法
     * 抽象方法用abstract修饰, 因为无法执行抽象方法，因此这个类也必须申明为抽象类（abstract class）
     * 使用abstract修饰的类就是抽象类,我们无法实例化一个抽象类
     * 抽象类本身被设计成只能用于被继承，因此，抽象类可以强迫子类实现其定义的抽象方法，否则编译会报错。因此，抽象方法实际上相当于定义了“规范”
     */
    public abstract void run();

    public void cry(){
        System.out.println("...");
    }

}

class Cat extends Animals {
    @Override
    public void run() {
        System.out.println("I'm a cat...");
    }
}

class Dog extends Animals {

    @Override
    public void run() {
        System.out.println("I'm a dog...");
    }
}

class Panda extends Animals {

    @Override
    public void run() {
        System.out.println("I'm a panda...");
    }
}