package com.motifsing.course.quickStart;

import java.util.Arrays;

/**
 * @ClassName ArraySort
 * @Description 数组排序
 * @Author Motifsing
 * @Date 2021/1/21 10:17
 * @Version 1.0
 **/
public class ArraySort {
    public static void main(String[] args) {
        // 冒泡排序
        int[] ns = {8, 3, 2, 5, 7, 6, 1, 4, 9};
        for (int i=0; i<ns.length - 1; i++) {
            for (int j=0; j<ns.length - i - 1; j++) {
                if (ns[j] > ns[j+1]) {
                    int tmp = ns[j+1];
                    ns[j+1] = ns[j];
                    ns[j] = tmp;
                }
            }
        }
        System.out.println(Arrays.toString(ns));

        // 内置排序
        int[] ns1 = { 28, 12, 89, 73, 65, 18, 96, 50, 8, 36 };
        Arrays.sort(ns1);
        System.out.println(Arrays.toString(ns1));

        // 降序排序
        int[] ns2 = { 28, 12, 89, 73, 65, 18, 96, 50, 8, 36 };
        // 排序前:
        System.out.println(Arrays.toString(ns2));
        for (int i=0; i<ns2.length - 1; i++) {
            for (int j=0; j<ns2.length - i - 1; j++) {
                if (ns2[j] < ns2[j+1]) {
                    int tmp = ns2[j+1];
                    ns2[j+1] = ns2[j];
                    ns2[j] = tmp;
                }
            }
        }

        // 排序后:
        System.out.println(Arrays.toString(ns2));
        if ("[96, 89, 73, 65, 50, 36, 28, 18, 12, 8]".equals(Arrays.toString(ns2))) {
            System.out.println("测试成功");
        } else {
            System.out.println("测试失败");
        }

    }
}
