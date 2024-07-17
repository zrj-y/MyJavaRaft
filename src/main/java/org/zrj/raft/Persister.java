package org.zrj.raft;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Persist Raft status.
 * Every node has its own Persister.
 */
public class Persister {

    private final Lock lock;
    private byte[] raftState;
    private byte[] snapshot;

    public Persister() {
        this.lock = new ReentrantLock();
    }

    // Todo:MIT源代码使用了浅拷贝，暂时不清楚是否正确，先按照源代码来
    public Persister copy() {
        lock.lock();
        try {
            Persister np = new Persister();
            np.raftState = raftState;
            np.snapshot = snapshot;
            return np;
        } finally {
            lock.unlock();
        }
    }

    public void saveRaftState(byte[] state) {
        lock.lock();
        try {
            raftState = state;
        } finally {
            lock.unlock();
        }
    }

    public byte[] readRaftState() {
        lock.lock();
        try {
            return raftState;
        } finally {
            lock.unlock();
        }
    }

    public int raftStateSize() {
        lock.lock();
        try {
            return raftState.length;
        } finally {
            lock.unlock();
        }
    }

    public void saveStateAndSnapshot(byte[] state, byte[] snapshot) {
        lock.lock();
        try {
            raftState = state;
            this.snapshot = snapshot;
        } finally {
            lock.unlock();
        }
    }

    public byte[] readSnapshot() {
        lock.lock();
        try {
            return snapshot;
        } finally {
            lock.unlock();
        }
    }

    public int snapshotSize() {
        lock.lock();
        try {
            return snapshot.length;
        } finally {
            lock.unlock();
        }
    }

}
