package org.zrj.raft;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import static org.zrj.raft.Role.Leader;

@Getter
@Builder
@ToString
public class State {
    private int term;
    private Role role;

    public boolean isLeader() {
        return Leader.equals(role);
    }
}
