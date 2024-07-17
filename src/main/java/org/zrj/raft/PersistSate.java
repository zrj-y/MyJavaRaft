package org.zrj.raft;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Builder
@Getter
public class PersistSate {
    private int currentTerm;
    private String votedFor;
    private List<LogEntry> log;
    private int voteAtTerm;
}
