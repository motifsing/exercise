package com.motifsing.rpc.rpc02;

import com.motifsing.rpc.entity.User;

/**
 * @Author： YangHW
 * @Date: 2022/2/7 15:11
 * @description: TODO 类描述
 */
public class Client {
    public static void main(String[] args) throws Exception {

        // 不再关注网络传输
        Stub stub = new Stub();
        User user = stub.getUserById(18);

        // 进行业务处理
        System.out.println(user);
    }
}
