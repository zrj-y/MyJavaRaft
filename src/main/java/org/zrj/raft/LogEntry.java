package org.zrj.raft;

public class LogEntry {
    private int term;
    private int index;
    private Command[] commands;
}
