package com.motifsing.proxy.static_proxy;

import com.motifsing.proxy.static_proxy.dao.IUserDao;
import com.motifsing.proxy.static_proxy.dao.impl.UserDaoImpl;
import com.motifsing.proxy.static_proxy.proxy.UserDaoProxy;
import org.junit.Test;

/**
 * @Author： YangHW
 * @Date: 2022/2/9 09:58
 * @description: 静态代理
 *     静态代理实现较简单，只要代理对象对目标对象进行包装，即可实现增强功能
 *     但静态代理只能为一个目标对象服务，如果目标对象过多，则会产生很多代理类;一旦接口增加方法，目标对象与代理对象都要进行修改
 *     静态代理在编译时产生class字节码文件，可以直接使用，效率高
 */
public class UserDaoTest {
    @Test
    public void testDelete() {
        //目标对象
        IUserDao target = new UserDaoImpl();
        //代理对象
        UserDaoProxy proxy = new UserDaoProxy(target);
        proxy.delete(1);
    }
}
