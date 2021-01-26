package com.motifsing.course.coreClass;

/**
 * @ClassName EnumMain
 * @Description 枚举类
 * @Author Motifsing
 * @Date 2021/1/26 18:15
 * @Version 1.0
 **/
public class EnumMain {
    public static void main(String[] args) {
        Weekday day = Weekday.SUN;
        if (day == Weekday.SAT || day == Weekday.SUN) {
            System.out.println("Work at home!");
        } else {
            System.out.println("Work at office!");
        }
        System.out.println(day.name());
        System.out.println(day.toString());
    }
}

//class Weekday {
//    public static final int SUN = 0;
//    public static final int MON = 1;
//    public static final int TUE = 2;
//    public static final int WED = 3;
//    public static final int THU = 4;
//    public static final int FRI = 5;
//    public static final int SAT = 6;
//}

enum Weekday {
    /**
     *
     */
    MON(1, "星期一"), TUE(2, "星期二"), WED(3, "星期三"),
    THU(4, "星期四"), FRI(5, "星期五"), SAT(6, "星期六"),
    SUN(0, "星期日");

    public final int dayValue;
    private final String chinese;

    private Weekday(int dayValue, String chinese) {
        this.dayValue = dayValue;
        this.chinese = chinese;
    }

    @Override
    public String toString() {
        return this.chinese;
    }
}