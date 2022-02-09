package com.motifsing.proxy.proxy_cglib.proxy;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;

/**
 * @Author： YangHW
 * @Date: 2022/2/9 13:24
 * @description: 代理实例创建工厂
 */
public class DaoProxyFactory {
    /**
     * 创建代理实例
     * @param <T>
     * @return
     */
    public static <T> T newInstance(Class<T> clz, MethodInterceptor interceptor){
        Enhancer enhancer=new Enhancer();       // CGLIB enhancer增强类对象
        enhancer.setSuperclass(clz);            // 设置被代理类
        enhancer.setCallback(interceptor);      // 设置代理逻辑对象
        return  (T) enhancer.create();
    }
}
