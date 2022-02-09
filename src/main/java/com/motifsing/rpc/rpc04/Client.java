package com.motifsing.rpc.rpc04;

import com.motifsing.rpc.entity.Person;
//import com.motifsing.rpc.entity.User;
import com.motifsing.rpc.service.IPersonService;
//import com.motifsing.rpc.service.IUserService;


/**
 * @Author： YangHW
 * @Date: 2022/2/7 15:27
 * @description: TODO 类描述
 */
public class Client {
    public static void main(String[] args) throws Exception {

        /*IUserService userService = (IUserService)Stub.getStub(IUserService.class);
        User user = userService.getUserByName("青石路");
        System.out.println(user);*/

        IPersonService personService = (IPersonService)Stub.getStub(IPersonService.class);
        Person person = personService.getPersonByPhoneNumber("123");
        System.out.println(person);
    }
}
