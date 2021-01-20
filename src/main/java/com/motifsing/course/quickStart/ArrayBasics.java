package com.motifsing.course.quickStart;

/**
 * @ClassName array
 * @Description 数组练习
 * @Author Administrator
 * @Date 2021/1/20 15:35
 * @Version 1.0
 **/
public class ArrayBasics {
    public static void main(String[] args) {
        // 数组是引用类型
        int[] ns1 = new int[3];

        ns1[0] = 1;
        ns1[1] = 2;
        ns1[2] = 3;

        int[] ns2 = new int[] {1, 2, 3};

        int[] ns3 = {1, 2, 3};

        String[] names = {"ABC", "XYZ", "zoo"};
        String s = names[1];
        names[1] = "cat";
        // s是"XYZ"还是"cat"?
        System.out.println(s);

        System.out.println(names[1]);
    }
}
