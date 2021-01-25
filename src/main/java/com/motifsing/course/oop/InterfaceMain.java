package com.motifsing.course.oop;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @ClassName InterfaceMain
 * @Description 接口
 * @Author Motifsing
 * @Date 2021/1/25 15:04
 * @Version 1.0
 **/
public class InterfaceMain {
    /**
     * 合理设计interface和abstract class的继承关系，可以充分复用代码。
     * 一般来说，公共逻辑适合放在abstract class中，具体逻辑放到各个子类，而接口层次代表抽象程度。
     * 在使用的时候，实例化的对象永远只能是某个具体的子类，但总是通过接口去引用它，因为接口比抽象类更抽象
     */
    public static void main(String[] args) {
        // 用List接口引用具体子类的实例
        List list = new ArrayList();
        Country c = new China();

        // 向上转型为Collection接口
        Collection coll = list;
        Country c1 = c;

        // 向上转型为Iterable接口
        Iterable it = coll;


    }
}

/**
 * 在抽象类中，抽象方法本质上是定义接口规范：即规定高层类的接口，从而保证所有子类都有相同的接口实现，这样，多态就能发挥出威力。
 * 如果一个抽象类没有字段，所有方法全部都是抽象方法
 * 因为接口定义的所有方法默认都是public abstract的，所以这两个修饰符不需要写出来
 *
 *             abstract class	           interface
 * 继承	       只能extends一个class	       可以implements多个interface
 * 字段	       可以定义实例字段	           不能定义实例字段(可以有static字段）
 * 抽象方法	   可以定义抽象方法	           可以定义抽象方法
 * 非抽象方法    可以定义非抽象方法	           可以定义default方法
 */
interface Country {

    String getName();

    /**
     * default方法
     * 在接口中，可以定义default方法。例如，把Country接口的run()方法改为default方法
     * default方法的目的是:
     *  实现类可以不必覆写default方法。(当我们需要给接口新增一个方法时，会涉及到修改全部子类。
     *      如果新增的是default方法，那么子类就不必全部修改，只需要在需要覆写的地方去覆写新增方法)
     * 接口可以定义default方法（JDK>=1.8）
     */

    default void run(){
        System.out.println("I'm default...");
    }


}

/**
 * 接口继承
 * 一个interface可以继承自另一个interface。interface继承自interface使用extends，它相当于扩展了接口的方法
 */
interface Area extends Country {
    int size();
}

/**
 * 当一个具体的class去实现一个interface时，需要使用implements关键字
 * 在Java中，一个类只能继承自另一个类，不能从多个类继承。但是，一个类可以实现多个interface
 */
class China implements Country {

    @Override
    public String getName() {
        return "China";
    }

    @Override
    public void run() {
        System.out.println("I'm china...");
    }
}