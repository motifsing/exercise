package com.motifsing.proxy.static_proxy.proxy;

import com.motifsing.proxy.static_proxy.dao.IUserDao;

/**
 * @Author： YangHW
 * @Date: 2022/2/9 09:49
 * @description: 代理逻辑在代理类中，而不是由用户自定义
 */
public class UserDaoProxy implements IUserDao {
    private IUserDao target;            // 被代理对象

    public UserDaoProxy(IUserDao target) {
        this.target = target;
    }

    /**
     *  前置/后置 处理一旦写完，就固定死了，后续想修改的话需要改此代理类
     * @param id
     * @return
     */
    public int delete(int id) {
        // 前置处理，例如开启事务
        System.out.println("前置处理...");

        // 调用目标对象方法
        int count = target.delete(id);

        // 后置处理，例如提交事务或事务回滚
        System.out.println("后置处理...");
        return count;
    }

}
