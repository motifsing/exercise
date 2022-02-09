package com.motifsing.rpc.rpc01;

import com.motifsing.rpc.entity.User;
import com.motifsing.rpc.rpc01.server.UserServiceImpl;
import com.motifsing.rpc.service.IUserService;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @Author： YangHW
 * @Date: 2022/2/7 14:46
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
        DataInputStream dis = new DataInputStream(in);
        DataOutputStream dos = new DataOutputStream(out);

        // 从 socket 读取参数
        int id = dis.readInt();
        System.out.println("id = " + id);

        // 查询本地数据
        IUserService userService = new UserServiceImpl();
        User user = userService.getUserById(id);

        // 往 socket 写响应值
        dos.writeInt(user.getId());
        dos.writeUTF(user.getName());
        dos.flush();

        dis.close();
        dos.close();
    }
}