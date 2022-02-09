package com.motifsing.rpc.rpco4;

import com.motifsing.rpc.rpco4.server.PersonServiceImpl;
import com.motifsing.rpc.rpco4.server.UserServiceImpl;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;


/**
 * @Author： YangHW
 * @Date: 2022/2/7 15:30
 * @description: TODO 类描述
 */
public class Server {
    private static boolean is_running = true;

    private static final HashMap<String, Object> REGISTRY_MAP = new HashMap();

    public static void main(String[] args) throws Exception {

        // 向注册中心注册服务
        REGISTRY_MAP.put("com.qsl.rpc.service.IUserService", new UserServiceImpl());
        REGISTRY_MAP.put("com.qsl.rpc.service.IPersonService", new PersonServiceImpl());

        ServerSocket serverSocket = new ServerSocket(8888);
        while (is_running) {
            System.out.println("等待 client 连接");
            Socket client = serverSocket.accept();
            System.out.println("获取到 client...");
            handle(client);
            client.close();
        }
        serverSocket.close();
    }

    private static void handle(Socket client) throws Exception {
        InputStream in = client.getInputStream();
        OutputStream out = client.getOutputStream();
        ObjectInputStream ois = new ObjectInputStream(in);
        ObjectOutputStream oos = new ObjectOutputStream(out);

        // 获取服务名
        String serviceName = ois.readUTF();
        System.out.println("serviceName = " + serviceName);

        // 获取方法名、方法的参数类型、方法的参数值
        String methodName = ois.readUTF();
        Class[] parameterTypes = (Class[]) ois.readObject();
        Object[] args = (Object[]) ois.readObject();

        // 获取服务；从服务注册中心获取服务
        Object serverObject = REGISTRY_MAP.get(serviceName);

        // 通过反射调用服务的方法
        Method method = serverObject.getClass().getMethod(methodName, parameterTypes);
        Object resp = method.invoke(serverObject, args);

        // 往 socket 写响应值；直接写可序列化对象（实现 Serializable 接口）
        oos.writeObject(resp);
        oos.flush();

        ois.close();
        oos.close();
    }
}
