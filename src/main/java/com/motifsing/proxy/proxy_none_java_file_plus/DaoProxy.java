package com.motifsing.proxy.proxy_none_java_file_plus;

import com.motifsing.proxy.proxy_none_java_file_plus.proxy.InvocationHandler;

import java.lang.reflect.Method;

/**
 * @Author： YangHW
 * @Date: 2022/2/9 10:34
 */
public class DaoProxy implements InvocationHandler {
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
        System.out.println("DaoProxy 操作....");
        Object result = null;  // sqlSession操作数据库后的返回结果，写死成null只是为了演示
        return result;
    }
}
