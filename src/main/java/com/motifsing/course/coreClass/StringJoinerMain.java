package com.motifsing.course.coreClass;

import java.util.StringJoiner;

/**
 * @ClassName StringJoinerMain
 * @Description
 * @Author Motifsing
 * @Date 2021/1/26 10:58
 * @Version 1.0
 **/
public class StringJoinerMain {
    public static void main(String[] args) {
        // StringJoiner
        StringJoiner sj = new StringJoiner(",", "Hello, ", "!");
        String[] names = {"Bob", "Alice", "Grace"};
        for (String name: names) {
            sj.add(name);
        }
        System.out.println(sj.toString());

        // join
        String s = String.join(", ", names);
        System.out.println(s);

        // SELECT语句
        String[] fields = { "name", "position", "salary" };
        String table = "employee";
        String select = buildSelectSql(table, fields);
        System.out.println(select);
        System.out.println("SELECT name, position, salary FROM employee".equals(select) ? "测试成功" : "测试失败");
    }

    /**
     * 使用StringJoiner构造一个SELECT语句
     */
    static String buildSelectSql(String table, String[] fields) {
        StringJoiner sj = new StringJoiner(", ", "SELECT ", " FROM " + table);
        for (String field:fields) {
            sj.add(field);
        }
        return sj.toString();
    }
}
