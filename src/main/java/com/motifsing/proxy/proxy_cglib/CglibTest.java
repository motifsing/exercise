package com.motifsing.proxy.proxy_cglib;

import com.motifsing.proxy.proxy_cglib.dao.IUserDao;
import com.motifsing.proxy.proxy_cglib.proxy.DaoMethodInterceptor;
import com.motifsing.proxy.proxy_cglib.proxy.DaoProxyFactory;
import net.sf.cglib.proxy.MethodInterceptor;
import org.junit.Test;

/**
 * @Author： YangHW
 * @Date: 2022/2/9 13:25
 * @description: cglib代理示例；cglib广泛的被许多AOP的框架使用，例如Spring AOP；
 *     spring中代理实现包括两种：JDK的动态代理和cglib的动态代理
 *         反射机制在生成类的过程中比较高效，而asm在生成类之后的相关执行过程中比较高效
 *         默认情况小，如果被代理的目标对象实现了至少一个接口，则会使用JDK动态代理。所有该目标类型实现的接口都将被代理。若该目标对象没有实现任何接口，则创建一个CGLIB代理
 *         当然，我们也可以进行配置，强制使用CGLIB代理
 *
 * cglib既能针对接口动态代理实现代理实例，也能针对实例对象生成代理对象
 * cglib代理无需实现接口，通过生成类字节码实现代理，比反射稍快，不存在性能问题，
 * cglib针对实例对象生成代理，会继承实例对象，需要重写方法，所以目标对象不能为final类
 */
public class CglibTest {
    @Test
    public void cglibTest() {
        MethodInterceptor interceptor = new DaoMethodInterceptor();

        IUserDao userDao = DaoProxyFactory.newInstance(IUserDao.class, interceptor);
        int result = userDao.delete(1);
        System.out.println("result = " + result);
    }
}
