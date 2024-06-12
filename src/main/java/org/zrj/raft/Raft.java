package org.zrj.raft;

import org.zrj.rpc.Node;
import org.zrj.rpc.RpcClient;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.zrj.raft.Role.*;

public class Raft implements Node {
    private Role role;
    private int votedFor;
    private int voteAtTerm;
    private int commitIndex;
    private int lastApplied;
    private List<Integer> nextIndex;
    private List<Integer> matchIndex;
    private List<LogEntry> log;
    private final RpcClient rpcClient;
    private final String nodeId;
    private volatile boolean stop;
    private final Lock lock;
    public Raft(String id, RpcClient rpcClient) {
        this.nodeId = id;
        this.rpcClient = rpcClient;
        this.votedFor = -1;
        this.voteAtTerm = -1;
        this.commitIndex = -1;
        this.lastApplied = -1;
        this.nextIndex = new ArrayList<>();
        this.matchIndex = new ArrayList<>();
        this.log = new ArrayList<>();
        this.role = Follower;
        this.lock = new ReentrantLock();
    }

    public void start() {
        rpcClient.getRegister().register(this);
        while(!stop) {
            if (Follower.equals(role)) {
                doFollower();
            } else if (Candidate.equals(role)) {
                doCandidate();
            } else if (Leader.equals(role)) {
                doLeader();
            } else {
                throw new RuntimeException(String.format("unknown role %s", role.name()));
            }
        }
    }

    private void doFollower() {
        long lastTime = System.currentTimeMillis();
        int electionTimeOut = ClusterConfig.MIN_ELECTION_TIMEOUT + new Random(ClusterConfig.MAX_ELECTION_TIMEOUT - ClusterConfig.MIN_ELECTION_TIMEOUT).nextInt();
        while(Follower.equals(role)) {
            long currentTime = System.currentTimeMillis();
            if(currentTime - lastTime > electionTimeOut) {
                role = Candidate;
            } else if (receiveHeartBeatRequest) {
                // 收到leader节点请求，重新开始等待选举
                lastTime = System.currentTimeMillis();
                receiveHeartBeatRequest = false;
            } else if () {

            }
        }
    }

    private void doCandidate() {
        while(Candidate.equals(role)) {

        }
    }

    private void doLeader() {
        while ((Leader.equals(role))) {

        }
    }

    @Override
    public String getId() {
        return nodeId;
    }

    private void rpcCall(String nodeId, String methodName, Object... args)
                        throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        rpcClient.call(nodeId, methodName, args);
    }
}
