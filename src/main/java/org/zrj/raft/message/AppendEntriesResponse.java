package org.zrj.raft.message;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class AppendEntriesResponse extends Message {
    private String nodeId;
    private boolean success;
    // 发起AppendEntriesRequest节点的term
    private int requesterTerm;
    // 收到AppendEntriesRequest节点的term
    private int responderTerm;
    private int matchIndex;
}
