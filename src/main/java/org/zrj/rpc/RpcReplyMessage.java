package org.zrj.rpc;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class RpcReplyMessage {
    private boolean ok;
    private Object reply;
}
