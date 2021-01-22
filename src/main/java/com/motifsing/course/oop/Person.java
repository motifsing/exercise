package com.motifsing.course.oop;

/**
 * @ClassName Person
 * @Description 一个类通过定义方法，就可以给外部代码暴露一些操作的接口，同时，内部自己保证逻辑一致性
 * @Author Motifsing
 * @Date 2021/1/21 11:30
 * @Version 1.0
 **/
public class Person {

    private int birth;

    private String name;

    private String[] names;

    public Person(){

    }

    public Person(String name, int birth){
        this.name = name;
        this.birth = birth;
    }

    /**
     * 一个构造方法可以调用其他构造方法，这样做的目的是便于代码复用。调用其他构造方法的语法是this(…)：
     */
    public Person(String name){
        this(name, 2000);
    }

    /**
     * 可变参数，传参数时("name1") 或者("name1", "name2")，可以保证无法传入null，因为传入0个参数时，接收到的实际值是一个空数组而不是null。
     * 也可以写成String[] names,但是传参时需要 new String[]{"name1", "name2"},并且可以传入null
     */
    public void setNames(String... names){
        this.names = names;
    }

    public String[] getNames(){
        return this.names;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        if (name == null || name.isEmpty()){
            throw new IllegalArgumentException("invalid name value.");
        }
        this.name = name;
    }

    /**
     * 重载方法
     */
    public void setName(){
        this.name = "default";
    }

    public int getBirth() {
        return birth;
    }

    public void setBirth(int birth) {
        this.birth = birth;
    }

    private int getCurrentAge(int currentYear){
        return currentYear - this.birth;
    }

    public int getAge(){
        return getCurrentAge(2020);
    }

    public int getAge(int currentYear) {
        return getCurrentAge(currentYear);
    }

}
