package com.motifsing.course.coreClass;

/**
 * @ClassName PackTypeMain
 * @Description 包装类型
 * @Author Motifsing
 * @Date 2021/1/26 17:14
 * @Version 1.0
 **/
public class PackTypeMain {
    /**
     * 基本类型：byte，short，int，long，boolean，float，double，char
     * 引用类型：所有class和interface类型
     *
     * 基本类型	对应的引用类型
     * boolean	java.lang.Boolean
     * byte	    java.lang.Byte
     * short	java.lang.Short
     * int	    java.lang.Integer
     * long	    java.lang.Long
     * float	java.lang.Float
     * double	java.lang.Double
     * char	    java.lang.Character
     */
    public static void main(String[] args) {
        // 引用类型可以赋值为null，表示空，但基本类型不能赋值为null
        String s = null;
        // int n = null; // compile error!

        // int和Integer互相转换
        Integer n1 = null;
        Integer n2 = new Integer(99);
        int n3 = n2.intValue();

        // Integer
        int i = 100;
        // 通过new操作符创建Integer实例(不推荐使用,会有编译警告):
        Integer n4 = new Integer(i);
        // 通过静态方法valueOf(int)创建Integer实例:
        Integer n5 = Integer.valueOf(i);
        // 通过静态方法valueOf(String)创建Integer实例:
        Integer n6 = Integer.valueOf("100");
        System.out.println(n5.intValue());

        // Auto Boxing

        // Java编译器可以帮助我们自动在int和Integer之间转型
        // 这种直接把int变为Integer的赋值写法，称为自动装箱（Auto Boxing），反过来，把Integer变为int的赋值写法，称为自动拆箱（Auto Unboxing）
        // 编译器自动使用Integer.valueOf(int)
        Integer n7 = 100;
        // 编译器自动使用Integer.intValue()
        int a = n7;

        // 不变类
        // 仔细观察结果的童鞋可以发现，==比较，较小的两个相同的Integer返回true，较大的两个相同的Integer返回false，
        // 这是因为Integer是不变类，编译器把Integer x = 127;自动变为Integer x = Integer.valueOf(127);
        // 为了节省内存，Integer.valueOf()对于较小的数，始终返回相同的实例，因此，==比较“恰好”为true，
        // 但我们绝不能因为Java标准库的Integer内部有缓存优化就用==比较，必须用equals()方法比较两个Integer
        Integer x = 127;
        Integer y = 127;
        Integer m = 99999;
        Integer n = 99999;
        // true
        System.out.println("x == y: " + (x==y));
        // false
        System.out.println("m == n: " + (m==n));
        // true
        System.out.println("x.equals(y): " + x.equals(y));
        // true
        System.out.println("m.equals(n): " + m.equals(n));
    }
}
