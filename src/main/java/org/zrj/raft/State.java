package org.zrj.raft;

public class State {
    private int term;
    private Role role;
    private int votedFor;
    private int voteAtTerm;
    private LogEntry[] log;
}
