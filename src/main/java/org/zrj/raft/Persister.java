package org.zrj.raft;

import com.google.gson.Gson;

import java.nio.charset.StandardCharsets;

/**
 * Persist Raft status.
 * Every node has its own Persister.
 */
public class Persister {
    private byte[] state;

    public void saveState(State state) {
        this.state = new Gson().toJson(state).getBytes(StandardCharsets.UTF_8);
    }

    public State readState() {
        return new Gson().fromJson(new String(state, StandardCharsets.UTF_8), State.class);
    }
}
