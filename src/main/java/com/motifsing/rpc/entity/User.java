package com.motifsing.rpc.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * @Author： YangHW
 * @Date: 2022/2/7 14:51
 */
@Setter
@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class User implements Serializable {
    private static final long serialVersionUID = 1L;

    private Integer id;
    private String name;
}
