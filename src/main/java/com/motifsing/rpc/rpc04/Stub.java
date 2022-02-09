package com.motifsing.rpc.rpc04;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.Socket;


/**
 * @Author： YangHW
 * @Date: 2022/2/7 15:27
 * @description: 动态代理，封装了网络数据传输
 */
public class Stub {
    public static Object getStub(Class clazz) {
        Object obj = Proxy.newProxyInstance(clazz.getClassLoader(), new Class[]{clazz}, new NetInvocationHandler(clazz));
        return obj;
    }

    static class NetInvocationHandler implements InvocationHandler {

        private Class clazz;

        NetInvocationHandler(Class clazz){
            this.clazz = clazz;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Socket s = new Socket("127.0.0.1", 8888);

            // 网络传输数据
            ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());

            // 传输接口名，告诉服务端，我要哪个服务
            oos.writeUTF(clazz.getName());

            // 传输方法名、方法参数类型、方法参数值；可能会有方法重载，所以要传参数列表
            oos.writeUTF(method.getName());
            Class[] parameterTypes = method.getParameterTypes();
            oos.writeObject(parameterTypes);
            oos.writeObject(args);

            // 从 socket 读响应值
            ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
            Object resp = ois.readObject();

            oos.close();
            ois.close();
            s.close();

            return resp;
        }
    }
}
