package org.zrj.raft;

import lombok.Getter;

@Getter
public class ApplyMsg {
    private boolean commandValid;
    private String command;
    private int commandIndex;
}
