# 包

## import
1. Java编译器最终编译出的.class文件只使用完整类名，因此，在代码中，当编译器遇到一个class名称时：
    a. 如果是完整类名，就直接根据完整类名查找这个class；
    b. 如果是简单类名，按下面的顺序依次查找：
    c. 查找当前package是否存在这个class；
    d. 查找import的包是否包含这个class；
    e. 查找java.lang包是否包含这个class。
2. 编写class的时候，编译器会自动帮我们做两个import动作：
    a. 默认自动import当前package的其他class；
    b. 默认自动import java.lang.*。
3. 如果有两个class名称相同，例如，mr.jun.Arrays和java.util.Arrays，那么只能import其中一个，另一个必须写完整类名。


# 作用域

## public
1. 定义为public的class、interface可以被其他任何类访问
2. 定义为public的field、method可以被其他类访问，前提是首先有访问class的权限

## private
1. 定义为private的field、method无法被其他类访问 (private访问权限被限定在class的内部，而且与方法声明顺序无关。
推荐把private方法放到后面，因为public方法定义了类对外提供的功能，阅读代码的时候，应该先关注public方法)
2. 由于Java支持嵌套类，如果一个类内部还定义了嵌套类，那么，嵌套类拥有访问private的权限

## protected
1. protected作用于继承关系。定义为protected的字段和方法可以被子类访问，以及子类的子类

## package
1. 包作用域是指一个类允许访问同一个package的没有public、private修饰的class，以及没有public、protected、private修饰的字段和方法(注意，包名必须完全一致，包没有父子关系)

## 局部变量
~~~java
package abc;

public class Hello {
    void hi(String name) { // ①
        String s = name.toLowerCase(); // ②
        int len = s.length(); // ③
        if (len < 10) { // ④
            int p = 10 - len; // ⑤
            for (int i=0; i<10; i++) { // ⑥
                System.out.println(); // ⑦
            } // ⑧
        } // ⑨
    } // ⑩
}
~~~
1. 在方法内部定义的变量称为局部变量，局部变量作用域从变量声明处开始到对应的块结束。方法参数也是局部变量
2. 我们观察上面的hi()方法代码：
   
   方法参数name是局部变量，它的作用域是整个方法，即①～⑩；
   
   变量s的作用域是定义处到方法结束，即②～⑩；
   
   变量len的作用域是定义处到方法结束，即③～⑩；
   
   变量p的作用域是定义处到if块结束，即⑤～⑨；
   
   变量i的作用域是for循环，即⑥～⑧。
   
   使用局部变量时，应该尽可能把局部变量的作用域缩小，尽可能延后声明局部变量。

## final
1. 用final修饰class可以阻止被继承
2. 用final修饰method可以阻止被子类覆写
3. 用final修饰field可以阻止被重新赋值
~~~java
package abc;

public class Hello {
    private final int n = 0;
    protected void hi() {
        this.n = 1; // error!
    }
}
~~~
4. 用final修饰局部变量可以阻止被重新赋值
~~~java
package abc;

public class Hello {
    protected void hi(final int t) {
        t = 1; // error!
    }
}
~~~

## 注意
1. 如果不确定是否需要public，就不声明为public，即尽可能少地暴露对外的字段和方法
2. 把方法定义为package权限有助于测试，因为测试类和被测试类只要位于同一个package，测试代码就可以访问被测试类的package权限方法。
3. 一个.java文件只能包含一个public类，但可以包含多个非public类。如果有public类，文件名必须和public类的名字相同
