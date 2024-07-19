package org.zrj.rpc;

import lombok.extern.slf4j.Slf4j;
import org.zrj.rpc.tool.Channel;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 只发送RequestMessage到消息队列，由NetWork接收消息然后通过reply channel返回响应
 */
@Slf4j
public class RpcClient {
    private final Random random;
    private final String name;
    private final String endName;
    private final Channel<RpcRequestMessage> ch;
    private final Network.Done network;

    public RpcClient(Channel<RpcRequestMessage> ch, String name, String endName, Network.Done network) {
        this.ch = ch;
        this.name = name;
        this.endName = endName;
        this.network = network;
        this.random = new Random();
    }

    // raft node调用call将请求发送给NetWork, NetWork通过reply channel返回响应
    public RpcReplyMessage call(String methodName, Object... args) {
        log.info("{} start to call other node {} {}", name, methodName, args);
        RpcRequestMessage rpcRequest = RpcRequestMessage.builder()
            .from(this.name)
            .endName(this.endName)
            .methodName(methodName)
            .args(args)
            .replyCh(new Channel<>())
            .build();
        if (network.isDone()) {
            return RpcReplyMessage.builder().ok(false).build();
        }
        // 将消息发送给Network
        ch.put(rpcRequest);
        // 阻塞等待Network响应
        log.info("{} succeed to call other node {} {}", name, methodName, args);
        RpcReplyMessage reply = rpcRequest.getReplyCh().poll(10 * 1000, TimeUnit.MILLISECONDS);
        if (reply == null) {
            log.info("{} fail get reply from other node {} {}", name, methodName, args);
        } else {
            log.info("{} succeed get reply from other node {} {}", name, methodName, args);
        }
        return reply;
    }

    @Override
    public String toString() {
        return "RpcClient{" +
            "name='" + name + '\'' +
            ", endName='" + endName + '\'' +
            '}';
    }
}
