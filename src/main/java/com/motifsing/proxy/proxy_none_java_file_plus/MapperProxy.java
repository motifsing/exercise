package com.motifsing.proxy.proxy_none_java_file_plus;

import com.motifsing.proxy.proxy_none_java_file_plus.proxy.InvocationHandler;

import java.lang.reflect.Method;

/**
 * @Author： YangHW
 * @Date: 2022/2/9 10:36
 * @description: TODO 类描述
 */
public class MapperProxy implements InvocationHandler {
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
        // TODO sqlSession 操作数据库，并获取执行结果
        System.out.println("数据库操作,并获取返回值...");
        Object result = null;  // sqlSession操作数据库后的返回结果，写死成null只是为了演示
        return result;
    }
}
