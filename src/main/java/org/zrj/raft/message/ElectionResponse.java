package org.zrj.raft.message;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class ElectionResponse extends Message {
    private Boolean vote;
    private int requesterTerm;
    private int responderTerm;
}
