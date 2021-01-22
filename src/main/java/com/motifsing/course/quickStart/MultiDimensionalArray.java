package com.motifsing.course.quickStart;

import java.util.Arrays;

/**
 * @ClassName TwoDimensionalArray
 * @Description 二维数组
 * @Author Motifsing
 * @Date 2021/1/21 10:48
 * @Version 1.0
 **/
public class MultiDimensionalArray {
    public static void main(String[] args) {
        // 创建二维数组
        int[][] ns = {
                { 1, 2, 3, 4 },
                { 5, 6, 7, 8 },
                { 9, 10, 11, 12 }
        };
        int[] arr0 = ns[0];
        System.out.println(arr0.length);

        // 二维数组的每个数组元素的长度并不要求相同
        int[][] ns2 = {
                { 1, 2, 3, 4 },
                { 5, 6 },
                { 7, 8, 9 }
        };
        System.out.println(Arrays.deepToString(ns2));
        
        // 遍历二维数组
        for (int[] arr : ns2) {
            for (int n: arr) {
                System.out.println(n);
            }
            
        }

        // 创建三维数组
        int[][][] ns3 = {
                {
                    {1, 2, 3}, {4, 5, 6}, {7, 8, 9}
                },
                {
                    {10, 11, 12, 13}, {14, 15}
                }
        };

        System.out.println(ns3.length);
        System.out.println(Arrays.deepToString(ns3[0]));
        System.out.println(Arrays.toString(ns3[0][0]));
        System.out.println(ns3[0][0][0]);
    }
}
