package com.motifsing.proxy.proxy_none_java_file_plus.mapper;

import com.motifsing.proxy.proxy_none_java_file_plus.model.User;

/**
 * @Author： YangHW
 * @Date: 2022/2/9 10:32
 * @description: TODO 类描述
 */
public interface UserMapper {
    Integer save(User user);

    User getUserById(Integer id);

    Integer delete(Integer id);
}
