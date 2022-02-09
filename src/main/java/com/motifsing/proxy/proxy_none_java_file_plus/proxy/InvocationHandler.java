package com.motifsing.proxy.proxy_none_java_file_plus.proxy;

import java.lang.reflect.Method;

/**
 * @Author： YangHW
 * @Date: 2022/2/9 10:33
 */
public interface InvocationHandler {
    public Object invoke(Object proxy, Method method, Object[] args);
}
