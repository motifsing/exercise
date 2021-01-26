package com.motifsing.course.coreClass;

/**
 * @ClassName StringBuilderMain
 * @Description StringBuilder
 * @Author Motifsing
 * @Date 2021/1/26 9:41
 * @Version 1.0
 **/
public class StringBuilderMain {
    /**
     * 对于普通的字符串+操作，并不需要我们将其改写为StringBuilder，因为Java编译器在编译时就自动把多个连续的+操作编码为StringConcatFactory的操作。
     * 在运行期，StringConcatFactory会自动把字符串连接操作优化为数组复制或者StringBuilder操作。
     *
     * 你可能还听说过StringBuffer，这是Java早期的一个StringBuilder的线程安全版本，它通过同步来保证多个线程操作StringBuffer也是安全的，
     * 但是同步会带来执行速度的下降。StringBuilder和StringBuffer接口完全相同，现在完全没有必要使用StringBuffer。
     */
    public static void main(String[] args) {
        // 此种方式虽然可以直接拼接字符串，但是，在循环中，每次循环都会创建新的字符串对象，然后扔掉旧的字符串。
        // 这样，绝大部分字符串都是临时对象，不但浪费内存，还会影响GC效率
        String s = "";

        for (int i = 0; i < 10; i++) {
            s = s + "," + i;
        }
        System.out.println(s);

        // 使用StringBuilder
        StringBuilder sb = new StringBuilder(1024);
        for (int i = 0; i < 10; i++) {
            sb.append(",");
            sb.append(i);
        }
        System.out.println(sb.toString());

        // 链式操作
        StringBuilder sb2 = new StringBuilder(1024);
        sb2.append("Mr ")
                .append("Bob")
                .append("!")
                .insert(0, "Hello, ");
        System.out.println(sb2.toString());

        // 自制链式操作类
        Adder adder = new Adder();
        adder.add(1).add(2).add(3).inc();
        System.out.println(adder.value());

        // 利用StringBuilder创建一个INSERT语句
        String[] fields = { "name", "position", "salary" };
        String table = "employee";
        String insert = buildInsertSql(table, fields);
        System.out.println(insert);
        String s1 = "INSERT INTO employee (name, position, salary) VALUES (?, ?, ?)";
        System.out.println(s1);
        System.out.println(s1.equals(insert) ? "测试成功" : "测试失败");
    }

    static String  buildInsertSql(String tableName, String[] fields) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ")
                .append(tableName)
                .append(" (");
        for (int i = 0; i < fields.length; i++) {
            sb.append(fields[i]);
            if (i < fields.length - 1) {
                sb.append(", ");
            }
        }
        sb.append(") VALUES (");
        for (int i = 0; i < fields.length - 1; i++) {
            sb.append("?, ");
        }
        sb.append("?)");
        return sb.toString();
    }
}

/**
 * 链式操作的类
 */
class Adder {
    private int sum = 0;

    public Adder add(int n) {
        sum += n;
        return this;
    }

    public Adder inc() {
        sum ++;
        return this;
    }

    public int value() {
        return sum;
    }
}
