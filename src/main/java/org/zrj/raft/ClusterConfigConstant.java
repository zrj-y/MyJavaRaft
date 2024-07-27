package org.zrj.raft;

public class ClusterConfigConstant {
    public static final int MIN_ELECTION_TIMEOUT = 150; // in milliseconds.
    public static final int MAX_ELECTION_TIMEOUT = 200; // in milliseconds.
    public static final int HEART_BEAT_TIMEOUT = 50; // in milliseconds.
    public static final int MAX_BATCH_SIZE = 200; // AppendEntriesRequest日志最大值
}
