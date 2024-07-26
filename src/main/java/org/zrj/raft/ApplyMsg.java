package org.zrj.raft;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
public class ApplyMsg {
    private boolean commandValid;
    private String command;
    private int commandIndex;
}
