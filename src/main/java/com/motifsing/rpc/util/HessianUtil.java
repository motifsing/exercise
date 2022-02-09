package com.motifsing.rpc.util;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import com.motifsing.rpc.entity.User;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

/**
 * @Author： YangHW
 * @Date: 2022/2/7 14:59
 * @Description: TODO 类描述
 */
public class HessianUtil {
    public static byte[] serialize(Object obj) throws Exception {
        ByteArrayOutputStream byteOs = new ByteArrayOutputStream();
        Hessian2Output hessian2Output = new Hessian2Output(byteOs);
        hessian2Output.writeObject(obj);
        hessian2Output.flush();
        byte[] bytes = byteOs.toByteArray();
        byteOs.close();
        hessian2Output.close();
        return bytes;
    }

    public static Object deserialize(byte[] bytes) throws Exception {
        ByteArrayInputStream byteIs = new ByteArrayInputStream(bytes);
        Hessian2Input hessian2Input = new Hessian2Input(byteIs);
        Object obj = hessian2Input.readObject();
        byteIs.close();
        hessian2Input.close();
        return obj;
    }

    public static byte[] jdkSerialize(Object obj) throws Exception {
        ByteArrayOutputStream byteOs = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(byteOs);
        oos.writeObject(obj);
        byte[] bytes = byteOs.toByteArray();
        byteOs.close();
        oos.close();
        return bytes;
    }

    // 序列化比较
    public static void main(String[] args) throws Exception {
        User user = new User(235, "张三");
        System.out.println("hessian : " + serialize(user).length);
        System.out.println("jdk : " + jdkSerialize(user).length);
    }
}
