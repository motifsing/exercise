package com.motifsing.course.oop;

import java.util.Arrays;

/**
 * @ClassName Method
 * @Description 执行入口
 * @Author Motifsing
 * @Date 2021/1/21 11:26
 * @Version 1.0
 **/
public class ExtendsMain {
    public static void main(String[] args) {
        Person person = new Person();
        person.setName("Motifsing");
        person.setBirth(20);
        System.out.println("name: " + person.getName() + "\n" + "age: " + person.getBirth());
        person.setNames("a");
        System.out.println(Arrays.toString(person.getNames()));
        person.setNames("a", "b");
        System.out.println(Arrays.toString(person.getNames()));

        // 基本类型参数绑定（基本类型参数的传递，是调用方值的复制。双方各自的后续修改，互不影响）
        int birth = 2000;
        person.setBirth(birth);
        System.out.println(person.getBirth());
        birth = 2010;
        System.out.println(person.getBirth());

        // 引用类型参数绑定(引用类型参数的传递，调用方的变量，和接收方的参数变量，指向的是同一个对象)
        // 双方任意一方对这个对象的修改，都会影响对方（因为指向同一个对象嘛）
        String[] fullname = new String[] { "Homer", "Simpson" };
        person.setNames(fullname);
        System.out.println(Arrays.toString(person.getNames()));
        fullname[0] = "Bart";
        System.out.println(Arrays.toString(person.getNames()));

        // 同样是引用类型，为什么name修改后，实例person的name没有发生变化（字符串不可变，只是name的引用指向变了，Bob是没有变的）
        String name = "Bob";
        person.setName(name);
        System.out.println(person.getName());
        name = "Jack";
        System.out.println(person.getName());

        // 向上转型
        Student s = new Student();
        Person p = new Person();

        // 把一个子类类型安全地变为父类类型的赋值，被称为向上转型（upcasting）
        Person p1 = new Student();
        Person p2 = s;
        Object o1 = p;
        Object o2 = s;

        // 向下转型(downcasting), 把一个父类类型强制转型为子类类型
        Student s3 = new Student();
        Person p3 = new Person();

        Student s4 = (Student) s3;
        // 不能把父类变为子类，因为子类功能比父类多，多的功能无法凭空变出来
        // 因此，向下转型很可能会失败。失败的时候，Java虚拟机会报ClassCastException
        // Student s5 = (Student) p3;

        // 通过instanceof操作符，可以先判断一个实例究竟是不是某种类型
        // instanceof实际上判断一个变量所指向的实例是否是指定类型，或者这个类型的子类。如果一个引用变量为null，那么对任何instanceof的判断都为false
        Person p7 = new Person();
        System.out.println(p7 instanceof Person);
        System.out.println(p7 instanceof Student);

        Student s7 = new Student();
        System.out.println(s7 instanceof Student);
        System.out.println(s7 instanceof Person);

        Student n = null;
        System.out.println(n instanceof Student);
    }
}

class Person {

    /**
     * 一个protected字段和方法可以被其子类，以及子类的子类所访问
     * 如果一个类不希望任何其他类继承自它，那么可以把这个类本身标记为final。用final修饰的类不能被继承
     * 对于一个类的实例字段，同样可以用final修饰。用final修饰的字段在初始化后不能被修改
     * class Person {
     *     public final String name;
     *     public Person(String name) {
     *         this.name = name;
     *     }
     * }
     */

    protected int birth;

    protected String name;

    protected String[] names;

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

    public void run(){
        System.out.println("Person is running...");
    }

}

class Student extends Person {

    /**
     * 子类自动获得了父类的所有字段，严禁定义与父类重名的字段！
     * 没有明确写extends的类，编译器会自动加上extends Object
     * Java只允许一个class继承自一个类，因此，一个类有且仅有一个父类。只有Object特殊，它没有父类
     * 子类无法访问父类的private字段或者private方法
     * 一个protected字段和方法可以被其子类，以及子类的子类所访问
     * 任何class的构造方法，第一行语句必须是调用父类的构造方法。如果没有明确地调用父类的构造方法，编译器会帮我们自动加一句super()
     * 如果父类没有默认的构造方法，子类就必须显式调用super()并给出参数以便让编译器定位到父类的一个合适的构造方法。
     * 即子类不会继承任何父类的构造方法。子类默认的构造方法是编译器自动生成的，不是继承的。
     */
    private int score;

    public Student(){}

    public Student(int score) {
        this.score = score;
    }

    public Student(String name, int score){
        super(name);
        this.score = score;

    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    @Override
    public void run(){
        System.out.println("Student is running...");
    }
}