package com.motifsing.proxy.proxy_jdk.proxy;

import java.lang.reflect.Proxy;

/**
 * @Author： YangHW
 * @Date: 2022/2/9 13:18
 * @description: mapper层接口的代理工厂，负责所有mapper层接口代理实例的生成
 *  * @param <T> 代理接口的类型，eg：UserMapper.cass
 */
public class MapperProxyFactory<T>  {
    private final Class<T> mapperInterface;

    public MapperProxyFactory(Class<T> mapperInterface) {
        this.mapperInterface = mapperInterface;
    }

    public T newInstance(MapperProxy<T> mapperProxy) {
        return (T) Proxy.newProxyInstance(this.mapperInterface.getClassLoader(), new Class[] {this.mapperInterface}, mapperProxy);
    }
}
