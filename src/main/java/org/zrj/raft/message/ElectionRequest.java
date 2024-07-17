package org.zrj.raft.message;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
public class ElectionRequest extends Message {
    @NonNull
    private String nodeId;
    private int term;
    private long LogIndex;

    @Override
    public String toString() {
        return "ElectionRequest{" +
            "nodeId=" + nodeId +
            ", term=" + term +
            ", LogIndex=" + LogIndex +
            '}';
    }
}
