package com.motifsing.rpc.rpc03;

import com.motifsing.rpc.entity.User;
import com.motifsing.rpc.rpc03.server.UserServiceImpl;
import com.motifsing.rpc.service.IUserService;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @Author： YangHW
 * @Date: 2022/2/7 15:21
 * @description: TODO 类描述
 */
public class Server {
    private static boolean is_running = true;

    public static void main(String[] args) throws Exception {
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

        // 获取方法名、方法的参数类型、方法的参数值
        String methodName = ois.readUTF();
        Class[] parameterTypes = (Class[]) ois.readObject();
        Object[] args = (Object[]) ois.readObject();

        IUserService userService = new UserServiceImpl();
        Method method = userService.getClass().getMethod(methodName, parameterTypes);
        User user = (User) method.invoke(userService, args);

        // 往 socket 写响应值；直接写可序列化对象（实现 Serializable 接口）
        oos.writeObject(user);
        oos.flush();

        ois.close();
        oos.close();
    }
}
