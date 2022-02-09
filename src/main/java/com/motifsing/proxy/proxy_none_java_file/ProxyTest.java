package com.motifsing.proxy.proxy_none_java_file;

import com.motifsing.proxy.proxy_none_java_file.mapper.UserMapper;
import com.motifsing.proxy.proxy_none_java_file.proxy.Proxy;
import org.junit.Test;

/**
 * @Author： YangHW
 * @Date: 2022/2/9 10:20
 * @description: 手动实现动态代理，不持久化中间的java源文件和.class文件到磁盘
 *     我们动态生成的类不以java文件的形式持久化到磁盘，而是直接在内存中编译、加载、生成实例对象
 *     注意：.java和.class还是动态生成了的，只是在内存中
 *
 * 引用了第三方编译工具
 *     // JavaCompiler.CompilationTask方式会生成class文件到磁盘
 *     // TODO JDK是如何实现的，是采用的另外的实现方式（如果是相同的方式，为什么没有对外的接口供调用）
 */
public class ProxyTest {
    @Test
    public void proxyTest() throws Exception {
        UserMapper userMapper = Proxy.newInstance(UserMapper.class);
        userMapper.getUserById(12);
    }
}
