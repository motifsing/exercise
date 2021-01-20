package com.motifsing.course.quickStart;

import java.util.Arrays;

/**
 * @ClassName TraverseArray
 * @Description 遍历数组
 * @Author Motifsing
 * @Date 2021/1/20 16:10
 * @Version 1.0
 **/
public class TraverseArray {
    public static void main(String[] args) {
        int[] ns = {1, 2, 3, 4, 5};

        // 遍历数组
        for (int i=0; i<ns.length; i++) {
            System.out.println(i + ": " + ns[i]);
        }

        // for each
        for (int n: ns) {
            System.out.println(n);
        }

        // 打印数组
        System.out.println(Arrays.toString(ns));

        // 倒序打印数组元素:
        for (int i=ns.length-1; i>=0; i--) {
            System.out.println(ns[i]);
        }
    }
}
