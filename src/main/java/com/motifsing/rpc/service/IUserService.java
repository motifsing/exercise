package com.motifsing.rpc.service;

import com.motifsing.rpc.entity.User;

/**
 * @Author： YangHW
 * @Date: 2022/2/7 14:58
 */
public interface IUserService {
    /**
     * 根据用户id获取用户信息
     */
    User getUserById(Integer id);

    /**
     * 根据用户名获取用户信息
     */
    User getUserByName(String name);
}
