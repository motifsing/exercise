package com.motifsing.flink.statistics.market.beans;

/**
 * @ClassName AdCountViewByProvince
 * @Description
 * @Author Motifsing
 * @Date 2021/3/30 11:59
 * @Version 1.0
 **/
public class AdCountViewByProvince {
    private String province;
    private String windowEnd;
    private Long count;

    public AdCountViewByProvince() {
    }

    public AdCountViewByProvince(String province, String windowEnd, Long count) {
        this.province = province;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    @Override
    public String toString() {
        return "AdCountViewByProvince{" +
                "province='" + province + '\'' +
                ", windowEnd='" + windowEnd + '\'' +
                ", count=" + count +
                '}';
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
