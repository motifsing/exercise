package com.motifsing.proxy.proxy_none_java_file_plus;

import org.junit.Test;

import com.motifsing.proxy.proxy_none_java_file_plus.mapper.UserMapper;
import com.motifsing.proxy.proxy_none_java_file_plus.proxy.InvocationHandler;
import com.motifsing.proxy.proxy_none_java_file_plus.proxy.Proxy;

/**
 * @Author： YangHW
 * @Date: 2022/2/9 13:12
 * @description: proxy-none-java-file中代理业务写死在了$Proxy0.java中了，
 *              也就是说$Proxy0.java是针对所有接口的同一类型的代理处理，这显然不对，我们需要的是通用的代理类；
 *              业务处理规范化，提供统一的规范接口：InvocationHandler，供用户自定义业务逻辑
 */
public class ProxyTest {
    @Test
    public void test() throws Exception {
        // InvocationHandler mapperProxy = new MapperProxy();
        InvocationHandler mapperProxy = new DaoProxy();
        UserMapper userMapper = Proxy.newInstance(UserMapper.class, mapperProxy);
        System.out.println(userMapper.delete(1));
        System.out.println("=================");
        System.out.println(userMapper.getUserById(1));
    }
}
