package org.zrj.raft.message;

import lombok.Getter;
import lombok.Setter;
import org.zrj.raft.LogEntry;

import java.util.List;

// leader节点心跳包
@Getter
@Setter
public class AppendEntriesRequest extends Message {
    private int term;
    private String leaderId;
    private int prevLogIndex;
    private int preLogTerm;
    private List<LogEntry> entries;
    private int leaderCommit;

    @Override
    public String toString() {
        return "AppendEntriesRequest{" +
            "term=" + term +
            ", leaderId=" + leaderId +
            ", prevLogIndex=" + prevLogIndex +
            ", preLogTerm=" + preLogTerm +
            ", entries=" + entries +
            ", leaderCommit=" + leaderCommit +
            '}';
    }
}
