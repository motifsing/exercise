package com.motifsing.rpc.rpc03.server;

import com.motifsing.rpc.entity.User;
import com.motifsing.rpc.service.IUserService;

/**
 * @Author： YangHW
 * @Date: 2022/2/7 15:22
 * @description: TODO 类描述
 */
public class UserServiceImpl implements IUserService {
    public User getUserById(Integer id) {
        // 实际应用中，应该是从数据库查
        return new User(id, "青石路");
    }

    public User getUserByName(String name) {

        // 实际应用中，从数据库查, 并实现相关业务处理
        return new User(888, name);
    }
}
