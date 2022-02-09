package com.motifsing.rpc.rpco4.server;

import com.motifsing.rpc.entity.Person;
import com.motifsing.rpc.service.IPersonService;

/**
 * @Author： YangHW
 * @Date: 2022/2/7 15:29
 * @description: TODO 类描述
 */
public class PersonServiceImpl implements IPersonService {
    @Override
    public Person getPersonById(Integer id) {
        return new Person(id, "青石路", "123456", "15174480311");
    }

    @Override
    public Person getPersonByPhoneNumber(String phoneNumber) {
        return new Person(88, "青石路", "123456", phoneNumber);
    }
}
