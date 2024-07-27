package org.zrj.raft.message;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class AppendEntriesResponse extends Message {
    private String nodeId;
    private boolean success;
    // 发起AppendEntriesRequest节点的term
    private int requesterTerm;
    // 收到AppendEntriesRequest节点的term
    private int responderTerm;
    private int matchIndex;
}
