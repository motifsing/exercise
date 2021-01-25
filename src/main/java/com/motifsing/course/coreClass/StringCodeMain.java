package com.motifsing.course.coreClass;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @ClassName AbstractMain
 * @Description 抽象类
 * @Author Motifsing
 * @Date 2021/1/25 22:58
 * @Version 1.0
 **/
public class StringCodeMain {
    /**
     * 对于不同版本的JDK，String类在内存中有不同的优化方式。具体来说，早期JDK版本的String总是以char[]存储，它的定义如下：
     *
     * public final class String {
     *     private final char[] value;
     *     private final int offset;
     *     private final int count;
     * }
     *
     * 而较新的JDK版本的String则以byte[]存储：如果String仅包含ASCII字符，则每个byte存储一个字符，否则，每两个byte存储一个字符，
     * 这样做的目的是为了节省内存，因为大量的长度较短的String通常仅包含ASCII字符：
     *
     * public final class String {
     *     private final byte[] value;
     *     private final byte coder; // 0 = LATIN1, 1 = UTF16
     */
    public static void main(String[] args) throws UnsupportedEncodingException {
        // String

        // String是一个引用类型， 它本身也是一个class
        String s1 = "Hello";

        // 实际上字符串在String内部是通过一个char[]数组表示的
        // 因为String太常用，所以Java用"..."字符串字面量表示方法
        String s2 = new String(new char[] {'H', 'e', 'l', 'l', 'o'});
        System.out.println(s2);

        // Java字符串的一个重要特点就是字符串不可变。
        // 这种不可变性是通过内部的private final char[]字段，以及没有任何修改char[]的方法实现的。
        // 新创建了一个字符串"HELLO", s1指向了"HELLO"
        System.out.println(s1);
        s1 = s1.toUpperCase();
        System.out.println(s1);

        // 字符串比较

        // 当我们想要比较两个字符串是否相同时，要特别注意，我们实际上是想比较字符串的内容是否相同。
        // 必须使用equals()方法而不能用==
        String s3 = "hello";
        String s4 = "hello";
        // 从表面上看，两个字符串用==和equals()比较都为true，但实际上那只是Java编译器在编译期，
        // 会自动把所有相同的字符串当作一个对象放入常量池，自然s1和s2的引用就是相同的。
        System.out.println(s3 == s4);
        System.out.println(s3.equals(s4));

        String s5 = "HELLO".toLowerCase();
        System.out.println(s3 == s5);
        System.out.println(s3.equals(s5));

        // 搜索字符串

        // 是否包含字符串
        // 注意到contains()方法的参数是CharSequence而不是String，因为CharSequence是String的父类。
        String s6 = "Hello";
        // true
        System.out.println(s6.contains("ll"));
        // 2
        System.out.println(s6.indexOf("l"));
        // 3
        System.out.println(s6.lastIndexOf("l"));
        // true
        System.out.println(s6.startsWith("He"));
        // true
        System.out.println(s6.endsWith("lo"));
        // 提取字符
        // llo
        System.out.println(s6.substring(2));
        // ll
        System.out.println(s6.substring(2, 4));

        // 去除首尾空白字符
        // 使用trim()方法可以移除字符串首尾空白字符。空白字符包括空格，\t，\r，\n
        // 注意：trim()并没有改变字符串的内容，而是返回了一个新字符串
        // "Hello"
        System.out.println("  \tHello\r\n ".trim());

        // 判断是否为空
        // true
        System.out.println("".isEmpty());
        // false
        System.out.println(" ".isEmpty());

        // 替换字串

        // 要在字符串中替换子串，有两种方法。一种是根据字符或字符串替换：
        String s7 = "hello";
        // "hewwo"，所有字符'l'被替换为'w'
        System.out.println(s7.replace('l', 'w'));
        // "he~~o"，所有子串"ll"被替换为"~~"
        System.out.println(s7.replace("ll", "~~"));

        // 通过正则表达式替换
        String s8 = "A,,B;C ,D";
        // "A,B,C,D"
        System.out.println(s8.replaceAll("[,;\\s]+", ","));

        // 分割字符串
        String s9 = "A,B,C,D";
        // {"A", "B", "C", "D"}
        String[] ss = s9.split(",");
        System.out.println(Arrays.toString(ss));

        // 拼接字符串, 使用静态方法join()，它用指定的字符串连接字符串数组
        // "A***B***C"
        String[] arr = {"A", "B", "C"};
        String s10 = String.join("***", arr);
        System.out.println(s10);

        // 格式化字符串

        /*
         * 有几个占位符，后面就传入几个参数。参数类型要和占位符一致
         * %s：显示字符串；
         * %d：显示整数；
         * %x：显示十六进制整数；
         * %f：显示浮点数。
         */

        String s11 = "Hi %s, your score is %.2f!";
        System.out.println(String.format(s11, "Bob", 59.5));

        // 类型转换

        // 要把任意基本类型或引用类型转换为字符串，可以使用静态方法valueOf()。这是一个重载方法，编译器会根据参数自动选择合适的方法
        // "123"
        System.out.println(String.valueOf(123));
        // "45.67"
        System.out.println(String.valueOf(45.67));
        // "true"
        System.out.println(String.valueOf(true));
        // 类似java.lang.Object@636be97c
        System.out.println(String.valueOf(new Object()));

        // 把字符串转换为int类型
        // 123
        int n1 = Integer.parseInt("123");
        System.out.println(n1);
        // 按十六进制转换，255
        int n2 = Integer.parseInt("ff", 16);
        System.out.println(n2);

        // 把字符串转换为boolean类型
        // true
        boolean b1 = Boolean.parseBoolean("true");
        System.out.println(b1);
        // false
        boolean b2 = Boolean.parseBoolean("FALSE");
        System.out.println(b2);

        // 转换为char[]
        // String -> char[]
        char[] cs = "Hello".toCharArray();
        System.out.println(cs);
        // char[] -> String
        String s12 = new String(cs);
        System.out.println(s12);
        // 如果修改了char[]数组，String并不会改变
        // 因为通过new String(char[])创建新的String实例时，它并不会直接引用传入的char[]数组，而是会复制一份，
        // 所以，修改外部的char[]数组不会影响String实例内部的char[]数组，因为这是两个不同的数组。
        cs[0] = 'h';
        System.out.println(s12);

        // 引用外部传入的数组，外部变量修改，会导致实例的字段值，所以类内部应该复制一份
        int[] scores = new int[] { 88, 77, 51, 66 };
        Score s = new Score(scores);
        s.printScores();
        scores[2] = 99;
        s.printScores();

        // 字符编码(Java的String和char在内存中总是以Unicode编码表示)
        // 按系统默认编码转换，不推荐
        byte[] b3 = "Hello".getBytes();
        System.out.println(Arrays.toString(b3));
        // 按UTF-8编码转换
        byte[] b4 = "Hello".getBytes("UTF-8");
        System.out.println(Arrays.toString(b4));
        // 按GBK编码转换
        byte[] b5 = "Hello".getBytes("GBK");
        System.out.println(Arrays.toString(b5));
        // 按UTF-8编码转换
        byte[] b6 = "Hello".getBytes(StandardCharsets.UTF_8);
        System.out.println(Arrays.toString(b6));

        // byte[]转换为String
        byte[] b = {72, 101, 108, 108, 111};
        // 按GBK转换
        String s14 = new String(b, "GBK");
        System.out.println(s14);
        // 按UTF-8转换
        String s15 = new String(b, StandardCharsets.UTF_8);
        System.out.println(s15);
    }
}

class Score {
    private final int[] scores;
    public Score(final int[] scores) {

        // 复制
        this.scores = scores.clone();
    }

    public void printScores() {
        System.out.println(Arrays.toString(scores));
    }
}
