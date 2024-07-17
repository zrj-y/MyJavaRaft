package org.zrj.rpc;

import lombok.Builder;
import lombok.Getter;
import org.zrj.rpc.tool.Channel;

@Getter
@Builder
public class RpcRequestMessage {
    // 发出请求的节点Name
    private String from;
    // caller和callee之间connect的name
    private String endName;
    private String methodName;
    private Object[] args;
    private Channel<RpcReplyMessage> replyCh;
}
