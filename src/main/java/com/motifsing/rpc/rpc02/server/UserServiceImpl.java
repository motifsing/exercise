package com.motifsing.rpc.rpc02.server;

import com.motifsing.rpc.entity.User;
import com.motifsing.rpc.service.IUserService;

/**
 * @Author： YangHW
 * @Date: 2022/2/7 15:08
 * @description: TODO 类描述
 */
public class UserServiceImpl implements IUserService {
    public User getUserById(Integer id) {
        // 实际应用中，应该是从数据库查
        return new User(id, "青石路");
    }

    public User getUserByName(String name) {
        return null;
    }
}
