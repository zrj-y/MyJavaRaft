package org.zrj.rpc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class RpcClient {

    private final Register register;

    public RpcClient(Register register) {
        this.register = register;
    }
    public void call(String nodeId, String methodName, Object... args) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Object object = register.getNode(nodeId);
        Class<?>[] parameterTypes = new Class<?>[args.length];
        for (int i = 0; i < args.length; i++) {
            parameterTypes[i] = args[i].getClass();
        }
        Method method = object.getClass().getMethod(methodName, parameterTypes);
        method.invoke(object, args);
    }

    public Register getRegister() {
        return register;
    }
}
