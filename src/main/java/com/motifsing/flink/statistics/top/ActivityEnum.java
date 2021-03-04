package com.motifsing.flink.statistics.top;

public enum ActivityEnum {
    /**
     * 活动id枚举类
     */
    TURNPLATE(1, "大转盘"), GUESS(2, "竞猜"), RED_PACKETS(3, "红包"),
    FISHING(4, "钓鱼"), ZILLIONAIRE(5, "大富翁"), ANSWER(6, "答题");

    private Integer id;
    private String name;

    ActivityEnum(Integer id, String name) {
        this.id = id;
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return this.name;
    }

    public static ActivityEnum randomType(ActivityEnum[] values){
        return values[(int)(Math.random() * values.length)];
    }
}
