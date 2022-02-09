package com.motifsing.rpc.rpc03;

import com.motifsing.rpc.entity.User;
import com.motifsing.rpc.service.IUserService;

/**
 * @Author： YangHW
 * @Date: 2022/2/7 15:23
 * @description: TODO 类描述
 */
public class Client {
    public static void main(String[] args) throws Exception {

        IUserService userService = Stub.getStub();
        //User user = userService.getUserById(23);

        User user = userService.getUserByName("李小龙");
        // 进行业务处理
        System.out.println(user);
    }
}
