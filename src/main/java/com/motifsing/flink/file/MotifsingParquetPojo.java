package com.motifsing.flink.file;

/**
 * @ClassName MotifsingParquetPojo
 * @Description
 * @Author Motifsing
 * @Date 2021/3/2 17:31
 * @Version 1.0
 **/
public class MotifsingParquetPojo {
    private String word;
    private Long count;

    public MotifsingParquetPojo() {
    }

    public MotifsingParquetPojo(String word, Long count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
