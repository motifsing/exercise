package com.motifsing.course.oop;

/**
 * @ClassName Student
 * @Description 学生类，继承Person
 * @Author Motifsing
 * @Date 2021/1/22 11:40
 * @Version 1.0
 **/
public class Student extends Person {

    /**
     * 子类自动获得了父类的所有字段，严禁定义与父类重名的字段！
     * 没有明确写extends的类，编译器会自动加上extends Object
     * Java只允许一个class继承自一个类，因此，一个类有且仅有一个父类。只有Object特殊，它没有父类
     */
    private int score;

    public Student(int score) {
        this.score = score;
    }

    public Student(String name, int age, int score){

    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }
}
