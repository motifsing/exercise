package com.motifsing.course.quickStart;

import java.util.Scanner;

/**
 * @ClassName InputAndOutput
 * @Description 输入和输出
 * @Author Motifsing
 * @Date 2021/1/20 16:17
 * @Version 1.0
 **/

public class InputAndOutput {
    public static void main(String[] args) {
        // 格式化输出
        double pi = 3.1415926;
        System.out.println(pi);
        System.out.printf("%.2f\n", pi);

        int n = 16;
        System.out.printf("%d\n", n);

        // 输入
        Scanner scanner = new Scanner(System.in);
        System.out.print("请输入你的名字：");
        String name = scanner.nextLine();
        System.out.print("请输入你的年龄：");
        int age = scanner.nextInt();
        System.out.printf("年龄：%s，年龄：%d\n", name, age);

    }
}
