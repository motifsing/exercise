package com.motifsing.proxy.static_proxy.dao.impl;

import com.motifsing.proxy.static_proxy.dao.IUserDao;

/**
 * @Author： YangHW
 * @Date: 2022/2/9 09:53
 */
public class UserDaoImpl implements IUserDao {
    public int delete(int id) {
        System.out.println("正常实现, 删除用户操作");
        return 0;
    }
}
