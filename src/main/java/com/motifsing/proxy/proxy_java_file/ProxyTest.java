package com.motifsing.proxy.proxy_java_file;

import com.motifsing.proxy.proxy_java_file.mapper.UserMapper;
import com.motifsing.proxy.proxy_java_file.proxy.Proxy;
import org.junit.Test;

/**
 * @Author： YangHW
 * @Date: 2022/2/9 10:09
 * @description: 手动实现动态代理，持久化中间的java源文件和.class文件到磁盘
 *     这个与我们平时的开发基本一致，先写java文件，编译成class，再加载class到内存，生成实例，最后操作实例完成任务
 *     只是说.java和.class是运行时动态生成的
 *     动态生成Java源文件并且排版是一个非常繁琐的工作，为了简化操作，我们可以使用JavaPoet这个第三方库帮我们生成代理对象的源码
 */
public class ProxyTest {
    @Test
    public void proxyTest() throws Exception {
        // Proxy.generateJavaFileByJavaPoet(UserMapper.class);
        UserMapper userMapper = Proxy.newInstance(UserMapper.class);
        userMapper.getUserById(12);
    }
}
