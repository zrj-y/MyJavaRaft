package org.zrj.raft.message;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
public abstract class Message {
    @NonNull
    private String tracingId;

    public void setTracingId() {
        UUID uuid = UUID.randomUUID();
        this.tracingId = uuid.toString().substring(0, 10);
    }
}
