package com.motifsing.rpc.service;

import com.motifsing.rpc.entity.Person;

/**
 * @Author： YangHW
 * @Date: 2022/2/7 14:55
 * @description: TODO 类描述
 */
public interface IPersonService {
    /**
     * 根据id获取
     * @param id
     * @return
     */
    Person getPersonById(Integer id);

    /**
     * 根据手机号获取
     * @param phoneNumber
     * @return
     */
    Person getPersonByPhoneNumber(String phoneNumber);
}
