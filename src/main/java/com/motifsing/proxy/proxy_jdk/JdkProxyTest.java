package com.motifsing.proxy.proxy_jdk;

import com.motifsing.proxy.proxy_jdk.mapper.UserMapper;
import com.motifsing.proxy.proxy_jdk.proxy.MapperProxy;
import com.motifsing.proxy.proxy_jdk.proxy.MapperProxyFactory;
import org.junit.Test;

/**
 * @Author： YangHW
 * @Date: 2022/2/9 13:19
 * @description: JDK动态代理示例
 *     模拟的mybatis中mapper代理实例的创建
 *     有兴趣的可以去看看mybatis的实现，mapper的代理实例的创建就是用的JDK的动态代理
 *
 * JDK的动态代理必须实现InvocationHandler接口完成代理业务逻辑的处理；
 * JDK的动态代理是通过反射来实现的，比较消耗系统性能，但可以减少代理类的数量，使用更灵活
 * JDK的动态代理有一个限制：被代理的对象必须实现一个或多个接口，如果针对接口来实现就不会有此限制
 */
public class JdkProxyTest {
    @Test
    public void testProxy() {

        // 代理逻辑处理handler
        MapperProxy mapperProxy = new MapperProxy(UserMapper.class);

        // 代理实例工厂
        MapperProxyFactory proxyFactory = new MapperProxyFactory(UserMapper.class);

        // 获取代理对象
        UserMapper mapper = (UserMapper) proxyFactory.newInstance(mapperProxy);

        int delete = mapper.delete(1);
        System.out.println("result = " + delete);
    }
}
