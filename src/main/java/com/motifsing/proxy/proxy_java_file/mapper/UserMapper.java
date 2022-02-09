package com.motifsing.proxy.proxy_java_file.mapper;

import com.motifsing.proxy.proxy_java_file.model.User;

/**
 * @Author： YangHW
 * @Date: 2022/2/9 10:04
 */
public interface UserMapper {
    Integer save(User user);

    User getUserById(Integer id);
}
