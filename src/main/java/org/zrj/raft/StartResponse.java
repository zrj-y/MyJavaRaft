package org.zrj.raft;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class StartResponse {
    private int commandIndex;
    private int term;
    private boolean leader;
}
