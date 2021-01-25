package com.motifsing.course.oop;

/**
 * @ClassName PolymorphicMain
 * @Description 多态
 * @Author Motifsing
 * @Date 2021/1/25 10:45
 * @Version 1.0
 **/
public class PolymorphicMain {
    public static void main(String[] args) {
        Person s = new Student();

        // 一个实际类型为Student，引用类型为Person的变量，调用其run()方法，调用的是Person还是Student的run()方法？
        // Java的实例方法调用是基于运行时的实际类型的动态调用，而非变量的声明类型
        // 这个非常重要的特性在面向对象编程中称之为多态。它的英文拼写: Polymorphic。
        // 多态是指，针对某个类型的方法调用，其真正执行的方法取决于运行时期实际类型的方法
        s.run();

        // 给一个有普通收入、工资收入和享受国务院特殊津贴的小伙伴算税:
        Income[] incomes = new Income[] {
                new Income(3000),
                new Salary(7500),
                new StateCouncilSpecialAllowance(15000)
        };
        System.out.println(totalTax(incomes));
    }

    public static double totalTax(Income... incomes) {
        double total = 0;
        for (Income income: incomes) {
            total = total + income.getTax();
        }
        return total;
    }
}

class Income {
    protected double income;

    public Income(double income) {
        this.income = income;
    }

    public double getTax() {
        // 税率10%
        return income * 0.1;
    }
}

class Salary extends Income {
    public Salary(double income) {
        super(income);
    }

    @Override
    public double getTax() {
        if (income <= 5000) {
            return 0;
        }
        return (income - 5000) * 0.2;
    }
}

class StateCouncilSpecialAllowance extends Income {
    public StateCouncilSpecialAllowance(double income) {
        super(income);
    }

    @Override
    public double getTax() {
        return 0;
    }
}