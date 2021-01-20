package com.motifsing.course.quickStart;

/**
 * @ClassName SwitchControl
 * @Description switch选择
 * @Author Motifsing
 * @Date 2021/1/20 17:11
 * @Version 1.0
 **/
public class LoopControl {
    public static void main(String[] args) {
        // 注意赋值语句要以;结束
        // switch的计算结果必须是整型、字符串或枚举类型；
        int option = 2;
        switch (option) {
            case 1:
                System.out.println("Selected 1");
                break;
            case 2:
            case 3:
                System.out.println("Selected 2, 3");
                break;
            default:
                System.out.println("Not selected");
                break;
        }

        // for循环,计算π π/4 = 1 - 1/3 + 1/5 - 1/7 + 1/9 - ...
        double pi = 0.0;
        for (int n=1, m=1; n<=100000; n+=2, m++){
            if (m % 2 == 1) {
                pi += (1.0 / n) * 4;
            } else {
                pi -= (1.0 / n) * 4;
            }
        }
        System.out.println(pi);

        // break,跳出当前循环，整个循环不再执行
        for (int i=1; i<=10; i++) {
            System.out.println("i = " + i);
            for (int j=1; j<=10; j++) {
                System.out.println("j = " + j);
                if (j >= i) {
                    break;
                }
            }
            // break跳到这里
            System.out.println("breaked");
        }

        // continue，跳出本次循环，继续下一次循环
        int sum = 0;
        for (int i=1; i<=10; i++) {
            System.out.println("begin i = " + i);
            if (i % 2 == 0) {
                continue; // continue语句会结束本次循环
            }
            sum = sum + i;
            System.out.println("end i = " + i);
        }
        System.out.println(sum); // 25
    }
}
