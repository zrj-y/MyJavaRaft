package org.zrj.raft;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.zrj.raft.message.AppendEntriesRequest;
import org.zrj.raft.message.ElectionRequest;
import org.zrj.raft.message.ElectionResponse;
import org.zrj.raft.message.Message;
import org.zrj.rpc.Node;
import org.zrj.rpc.RpcClient;
import org.zrj.rpc.tool.Channel;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.zrj.raft.Role.Candidate;
import static org.zrj.raft.Role.Follower;
import static org.zrj.raft.Role.Leader;

@Slf4j
public class Raft implements Node {
    // 原课程代码就存在的变量
    private final Lock lock;
    private final Map<String, RpcClient> peers;
    private final Persister persister;
    private final String me;
    private int dead;

    // 自己添加的变量
    private final Channel<ApplyMsg> applyCh; // 测试模块需要，其主动监听applyCh的数据
    private volatile Role role;
    private int term;
//    private long lastElectionTime;

    private String votedFor;
    private int voteAtTerm;

    private int commitIndex;
    private int lastApplied;
    // 其他节点下一个日志下标
    private final Map<String, Integer> nextIndex;
    // 其他节点已经匹配的日志下标
    private final Map<String, Integer> matchIndex;
    private List<LogEntry> logs;

    private volatile boolean stop;
    private int upvoteCount;
    private final Lock electionTimeOutLock;
    private volatile long lastReceivingHeartBeatTime;

    public Raft(Map<String, RpcClient> peers, String me, Persister persister, Channel<ApplyMsg> applyCh) {
        this.lock = new ReentrantLock();
        this.peers = peers;
        this.me = me;
        this.persister = persister;
        this.applyCh = applyCh;

        this.logs = new ArrayList<>();
        this.nextIndex = new HashMap<>();
        this.matchIndex = new HashMap<>();
        this.votedFor = null;
        this.voteAtTerm = -1;
        this.commitIndex = -1;
        this.lastApplied = -1;
        this.upvoteCount = 0;
        this.term = 0;
        this.role = Follower;
        this.electionTimeOutLock = new ReentrantLock();
        readPersist(persister.readRaftState());
        new Thread(() -> init(), String.format("Raft-%s-start-thread-", me)).start();

    }

    public State getState() {
        return State.builder().term(term).role(role).build();
    }

    public void persist() {
        PersistSate persistSate = PersistSate.builder()
            .currentTerm(term)
            .votedFor(votedFor)
            .log(logs)
            .voteAtTerm(voteAtTerm)
            .build();
        persister.saveRaftState(new Gson().toJson(persistSate).getBytes(StandardCharsets.UTF_8));
    }

    public void readPersist(byte[] data) {
        if (data == null || data.length < 1) { // bootstrap without any state?
            return;
        }
        PersistSate persistSate = new Gson().fromJson(new String(data, StandardCharsets.UTF_8), PersistSate.class);
        this.term = persistSate.getCurrentTerm();
        this.logs = Optional.ofNullable(persistSate.getLog()).orElse(new ArrayList<>());
        this.votedFor = persistSate.getVotedFor();
        this.voteAtTerm = persistSate.getVoteAtTerm();
    }


    public StartResponse start(String command) {
        int index = -1;
        int term = -1;
        boolean isLeader = true;
        lock.lock();
        if (Leader.equals(role)) {
            LogEntry logEntry = LogEntry.builder()
                .term(term)
                .index(logs.size())
                .command(command)
                .build();
            this.logs.add(logEntry);
        }
        lock.unlock();
        return StartResponse.builder().commandIndex(index).term(term).leader(isLeader).build();
    }

    private void init() {
        log.info("init " + me);
        doFollower();
    }

    private void doFollower() {
        log.info("doFollower {} at {}", me, term);
        role = Follower;
        // 其实论文中没有提这儿需要更新lastReceivingHeartBeatTime，不更新也不影响算法正确性，但是会影响算法性能。A还是Follower，
        // 快要转成Candidate的时候收到B Candidate的消息，如果没有A重置时间，即使已经处理完B的消息了新执行doFollower()，上个执行doFollower的线程
        // 可能转到Candidate
        lastReceivingHeartBeatTime = System.currentTimeMillis();
        int electionTimeOut = getElectionTimeOut();
        ScheduledExecutorService delayTask = Executors.newScheduledThreadPool(1);
        log.info("{} set electionTimeOut {} at {}", me, electionTimeOut, term);
        delayTask.schedule(() -> {
            long current = System.currentTimeMillis();
            if (current - lastReceivingHeartBeatTime >= electionTimeOut && role.equals(Follower)) {
                doCandidate();
            }
        }, electionTimeOut, TimeUnit.MILLISECONDS);
        delayTask.shutdown();
    }

    // 如果先收到heart beat包，但是还没有执行role = follower
    // 节点有可能变成candidate，或者仍在执行electionTask
    // 这种情况虽然不是很符合线程安全，期望先发生的事情处理完才处理后发生的事情，先处理完heart beat请求，再执行electionTimeOut事件
    // 但是不需要额外处理，相当于heart beat发慢了，处理起来需要加锁，没必要都弄成原子操作
    private void doCandidate() {
        log.info("doCandidate {} at {}", me, term);
        role = Candidate;
        // 先收到leader term更新成10，变成了Follower，
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        Runnable electionTask = new Runnable() {
            @Override
            public void run() {
                /*
                 * 收到leader的消息，term变成request.term。然后给term+1，这时候发送的term数据不正确
                 * sendElectionRequest()本身会异步执行，保证这次发送即使阻塞了也不影响下次electionTimeOut再次执行sendElectionRequest()
                 */
                lock.lock();
                if (Candidate.equals(role) && !stop) {
                    term += 1; // 收到voteResponse比较term并处理需要和这互斥
                    voteAtTerm = term;
                    votedFor = me;
                    upvoteCount = 1; // 重置投票数为1，自己投自己一票
                    // 这个地方加锁是因为其他线程会修改term, log
                    ElectionRequest electionRequest = new ElectionRequest();
                    electionRequest.setNodeId(me);
                    electionRequest.setTerm(term);
                    electionRequest.setLogIndex(logs.size() - 1);
                    electionRequest.setTracingId();
                    // 异步发送消息
                    broadCastAsync("handleElectionRequest", electionRequest);
                    lock.unlock();
                    int electionTimeOut = getElectionTimeOut();
                    log.info("{} set electionTimeOut {} at {}", me, electionTimeOut, term);
                    scheduler.schedule(this, electionTimeOut, TimeUnit.MILLISECONDS);
                } else {
                    lock.unlock();
                    // 不会立即shutdown所有线程，只是不接受任务，允许线程执行完自己结束。未调用shutdown则线程一直打开
                    scheduler.shutdown();
                }
            }
        };
        scheduler.schedule(electionTask, 0, TimeUnit.MILLISECONDS);
    }

    private int getElectionTimeOut() {
        return ClusterConfigConstant.MIN_ELECTION_TIMEOUT + new Random().nextInt(ClusterConfigConstant.MAX_ELECTION_TIMEOUT - ClusterConfigConstant.MIN_ELECTION_TIMEOUT + 1);
    }

    public void handleElectionRequest(ElectionRequest request) {
        ElectionResponse response = new ElectionResponse();
        int requesterTerm = request.getTerm();
        response.setRequesterTerm(requesterTerm);
        response.setTracingId(request.getTracingId());

        // 得加锁
        // 1. 收到leader节点请求刚把term改大，然后这里执行到term = requesterTerm，导致term被错误更新
        // 2. 重新成为candidate时候会更新term
        lock.lock();
        if (requesterTerm < term) {
            log.info("{} at term {} grant request from {} false, requesterTerm {}, my term {}", me, term, request.getNodeId(), requesterTerm, term);
            response.setVote(false);
            response.setResponderTerm(term);
            rpcCallAsync(request.getNodeId(), "handleElectionResponse", response);
        } else if (requesterTerm > term) {
            response.setVote(true);
            log.info("{} at term {} grant request from {} true, requesterTerm {}, my term {}", me, term, request.getNodeId(), requesterTerm, term);
            term = requesterTerm;
            response.setResponderTerm(term);
            voteAtTerm = term;
            votedFor = request.getNodeId();
            rpcCallAsync(request.getNodeId(), "handleElectionResponse", response);
            doFollower();
        } else {
            // 使用voteFor == null判断比较麻烦，还需要每次更新term的时候将voteFor设置为null
            // 使用voteAtTerm只需要在这里更新就可以了
            boolean grantVote = (voteAtTerm != term || votedFor == null);
            log.info("{} at term {} grant request from {} {}, requesterTerm {}, my voteFor {}, my voteAtTerm {}", me, term, request.getNodeId(), grantVote, requesterTerm, votedFor, voteAtTerm);
            response.setVote(grantVote);
            response.setResponderTerm(term);
            rpcCallAsync(request.getNodeId(), "handleElectionResponse", response);
            if (grantVote) {
                voteAtTerm = term;
                votedFor = request.getNodeId();
                doFollower();
            }
        }
        lock.unlock();
    }

    public void handleElectionResponse(ElectionResponse response) {
        // 得加锁
        // 1. 收到leader节点请求刚把term改大，然后这里执行到term = requesterTerm，导致term被错误更新
        // 2. 重新成为candidate时候会更新term
        lock.lock();
        log.info("{} at term {} handleElectionResponse {}", me, term, response);
        // 这个response可能是去年发出的request的响应
        if (response.getRequesterTerm() != term) {
            log.info("{} at term {} handleElectionResponse {} return1", me, term, response);
            return;
        }
        if (!response.getVote()) {
            if (response.getResponderTerm() > term) {
                term = response.getRequesterTerm();
                // todo 提前结束
                log.info("{} at term {} handleElectionResponse {} return2", me, term, response);
            }
        } else {
            upvoteCount++;
        }
        log.info("{} at term {} handleElectionResponse {} upvoteCount {}", me, term, response, upvoteCount);
        // 保证是这个term的Leader, 正要执行role=Leader时候，到了electionTimeOut时间,变成candidate，term+1。虽然role仍然会被更新成leader，但是term已经变了
        if (upvoteCount >= (peers.size() / 2 + 1)) {
            // 是否需要与doCandidate()的role=Candidate互斥，在role=Candidate加上lock.lock()?
            // 不需要。因为role=Candidate与role = Leader只会修改role，不会修改term。先执行role=Candidate, role=Leader或者role=Follower不影响算法正确性，
            // 只要保证节点确实是这个term的某个role，同一个term不会作为不同role更新数据就行，比如term 2时是leader，发出的heartbeat包包含的term不会是3

            // 为什么把role=Leader放到这里，包含在lock中？如果放到doLeader()中发出的心跳包的term可能不正确。lock锁住所有更新term的操作
            if (role != Leader) {
                log.info("{} at term {} handleElectionResponse {} upvoteCount {} turn to Leader", me, term, response, upvoteCount);
                role = Leader;
                doLeader();
            }
        }
        lock.unlock();
    }

    private void doLeader() {
        log.info("doLeader {} at {}, sate", me, term);
        // 以免doLeader()又被转换成candidate
        lastReceivingHeartBeatTime = System.currentTimeMillis();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        Runnable broadCastHeartBeat = new Runnable() {
            @Override
            public void run() {
                lock.lock();
                if (Leader.equals(role) && !stop) {
                    AppendEntriesRequest heartBeatRequest = new AppendEntriesRequest();
                    heartBeatRequest.setTracingId();
                    heartBeatRequest.setLeaderId(me);
                    heartBeatRequest.setTerm(term);
                    heartBeatRequest.setLeaderCommit(0);
                    heartBeatRequest.setPrevLogIndex(0);
                    heartBeatRequest.setPreLogTerm(0);
                    heartBeatRequest.setEntries(logs);
                    broadCastAsync("handleHeartBeatRequest", heartBeatRequest);
                    scheduler.schedule(this, ClusterConfigConstant.HEART_BEAT_TIMEOUT, TimeUnit.MILLISECONDS);
                } else {
                    scheduler.shutdown();
                }
                lock.unlock();
            }
        };
        scheduler.schedule(broadCastHeartBeat, ClusterConfigConstant.HEART_BEAT_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    public void handleHeartBeatRequest(AppendEntriesRequest heartBeatRequest) {
        lock.lock();
        if (heartBeatRequest.getTerm() >= term) {
            term = heartBeatRequest.getTerm();
            doFollower();
        }
        lock.unlock();
    }

    @Override
    public String getId() {
        return me;
    }

    /**
     * Raft中A调用B，A不需要等待B的响应。试想分布式算法需要等待响应是否还足够分布式。
     * A调用B，B收到请求后，处理完主动调用A。
     * 一种办法是让A将回调函数传给B，B处理请求后主动调用该回调函数。但是
     * 1. 传入回调函数的代码很丑陋
     * 2. 本身就是在实现Raft，B应该知道怎么处理请求，知道处理完请求后应该做什么，并不需要A告知
     */
    public void broadCastAsync(String methodName, Message args) {
        for (Map.Entry<String, RpcClient> entry : peers.entrySet()) {
            String peerName = entry.getKey();
            if (me.equals(peerName)) {
                continue;
            }
            log.info("me {} call other peer {} {}", me, peerName, args);
            RpcClient peer = entry.getValue();
            Runnable runnable = () -> peer.call(methodName, args);
            // 可以通过其他手段控制线程池最长执行时间，避免线程太多。
            new Thread(runnable, "call-peer-thread").start();
        }
    }

    public void rpcCallAsync(String nodeId, String methodName, Object... args) {
        log.info("me {} call other peer {} {} ", me, nodeId, args);
        new Thread(() -> peers.get(nodeId).call(methodName, args)).start();
    }

    public void kill() {
        stop = true;
    }
}
