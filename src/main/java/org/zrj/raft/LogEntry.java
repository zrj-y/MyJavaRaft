package org.zrj.raft;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class LogEntry {
    private final int term;
    private final int index;
    private final String command;
    public LogEntry(int term, int index, String command) {
        this.term = term;
        this.index = index;
        this.command = command;
    }
}
