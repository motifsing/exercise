package com.motifsing.rpc.rpc01;

import com.motifsing.rpc.entity.User;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

/**
 * @Author： YangHW
 * @Date: 2022/2/7 15:01
 * @description: TODO 类描述
 */
public class Client {
    public static void main(String[] args) throws Exception {
        Socket s = new Socket("127.0.0.1", 8888);

        // 网络传输数据
        // 往 socket 写请求参数
        DataOutputStream dos = new DataOutputStream(s.getOutputStream());
        dos.writeInt(18);
        // 从 socket 读响应值
        DataInputStream  dis = new DataInputStream(s.getInputStream());
        int id = dis.readInt();
        String name = dis.readUTF();
        // 将响应值封装成 User 对象
        User user = new User(id, name);
        dos.close();
        dis.close();
        s.close();

        // 进行业务处理
        System.out.println(user);
    }
}
