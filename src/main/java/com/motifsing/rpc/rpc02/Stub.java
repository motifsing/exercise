package com.motifsing.rpc.rpc02;

import com.motifsing.rpc.entity.User;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

/**
 * @Author： YangHW
 * @Date: 2022/2/7 15:12
 * @description: 相当于一个静态代理，封装了网络数据传输
 */
public class Stub {
    public User getUserById(Integer id) throws Exception {
        Socket s = new Socket("127.0.0.1", 8888);

        // 网络传输数据
        // 往 socket 写请求参数
        DataOutputStream dos = new DataOutputStream(s.getOutputStream());
        dos.writeInt(id);
        // 从 socket 读响应值
        DataInputStream dis = new DataInputStream(s.getInputStream());
        int userId = dis.readInt();
        String name = dis.readUTF();
        // 将响应值封装成 User 对象
        User user = new User(userId, name);
        dos.close();
        dis.close();
        s.close();

        return user;
    }
}
