package org.zrj.rpc;

import lombok.extern.slf4j.Slf4j;
import org.zrj.raft.Raft;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class Server {
    private final Lock lock;
    // 大小为1
    private Object service;
    // incoming RPC
    private int count;

    public Server() {
        this.lock = new ReentrantLock();
        this.service = null;
    }

    public void addService(Raft node) {
        service = node;
    }

    public RpcReplyMessage dispatch(RpcRequestMessage rpcRequest) {
        lock.lock();
        String serviceName = rpcRequest.getFrom();
        count++;
        lock.unlock();
        if (service == null) {
            throw new RuntimeException(String.format("Server.dispatch(): unknown %s.%s;\n", serviceName, rpcRequest.getMethodName()));
        }
        Object[] args = rpcRequest.getArgs();
        String methodName = rpcRequest.getMethodName();
        Class<?>[] parameterTypes = new Class<?>[args.length];
        for (int i = 0; i < args.length; i++) {
            parameterTypes[i] = args[i].getClass();
        }
        try {
            Method method = service.getClass().getMethod(methodName, parameterTypes);
            Object response = method.invoke(service, args);
            return RpcReplyMessage.builder().reply(response).ok(true).build();
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public int getCount() {
        try {
            lock.lock();
            return count;
        } finally {
            lock.unlock();
        }
    }
}
