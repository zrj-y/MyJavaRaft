package org.zrj.raft;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.zrj.raft.message.AppendEntriesRequest;
import org.zrj.raft.message.AppendEntriesResponse;
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

    // 自己添加的变量
    private final Channel<ApplyMsg> applyCh; // 测试模块需要，其主动监听applyCh的数据
    private volatile Role role;
    private int term;
    private String votedFor;
    private int voteAtTerm;
    private int commitIndex;
    private int lastApplied;
    // 其他节点下一个日志下标
    private final Map<String, Integer> nextIndexMap;
    // 其他节点已经匹配的日志下标
    private final Map<String, Integer> matchIndexMap;
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
        this.nextIndexMap = new HashMap<>();
        this.matchIndexMap = new HashMap<>();
        for (String peer : peers.keySet()) {
            nextIndexMap.put(peer, 0);
            matchIndexMap.put(peer, -1);
        }
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
        StartResponse response = StartResponse.builder().commandIndex(-1).term(-1).leader(false).build();
        lock.lock();
        if (Leader.equals(role)) {
            LogEntry logEntry = LogEntry.builder()
                .term(this.term)
                .index(logs.size())
                .command(command)
                .build();
            logs.add(logEntry);
            response = StartResponse.builder().commandIndex(logs.size() - 1).term(this.term).leader(true).build();
        }
        log.info("{} {} start cmd {}", role, me, command);
        lock.unlock();
        return response;
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
        ScheduledExecutorService delayTask = Executors.newScheduledThreadPool(1,
            new BasicThreadFactory.Builder().namingPattern("Follower-pool-")
                .uncaughtExceptionHandler((t, e) -> {
                        log.error(e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                ).build()
        );
        log.info("{} {} set electionTimeOut {} at term {}", me, role, electionTimeOut, term);
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
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1,
            new BasicThreadFactory.Builder().namingPattern("Candidate-pool-")
                .uncaughtExceptionHandler((t, e) -> {
                        log.error(e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                ).build()
        );
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
                    int lastLogIndex = logs.size() - 1;
                    int lastLogTerm = getLogTerm(lastLogIndex);
                    electionRequest.setLastLogIndex(lastLogIndex);
                    electionRequest.setLastLogTerm(lastLogTerm);
                    electionRequest.setTracingId();
                    // 异步发送消息
                    broadCastAsync("handleElectionRequest", electionRequest);
                    lock.unlock();
                    int electionTimeOut = getElectionTimeOut();
                    log.info("{} {} set electionTimeOut {} at term {}", me, role, electionTimeOut, term);
                    scheduler.schedule(this, electionTimeOut, TimeUnit.MILLISECONDS);
                } else {
                    lock.unlock();
                    // 不会立即shutdown所有线程，只是不接受任务，允许线程执行完自己结束。未调用shutdown则线程一直打开
                    log.info("{} stop!!!", me);
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
        } else {
            // 使用voteFor == null判断比较麻烦，还需要每次更新term的时候将voteFor设置为null
            // 使用voteAtTerm只需要在这里更新就可以了
            boolean hasNotVoted = (voteAtTerm != term || votedFor == null);
            boolean isLatestLog = isLatestLog(request);
            boolean grantVote = hasNotVoted && isLatestLog;
            log.info("{} at term {} grant request from {} {}, requesterTerm {}, my voteFor {}, my voteAtTerm {}, hasNotVoted {}, isLatestLog {}", me, term, request.getNodeId(), grantVote, requesterTerm, votedFor, voteAtTerm, hasNotVoted, isLatestLog);
            response.setVote(grantVote);
            if (grantVote || requesterTerm > term) {
                if (requesterTerm > term) {
                    term = requesterTerm;
                }
                if (grantVote) {
                    voteAtTerm = term;
                    votedFor = request.getNodeId();
                }
                doFollower();
            }
            response.setResponderTerm(term);
            rpcCallAsync(request.getNodeId(), "handleElectionResponse", response);
        }
        lock.unlock();
    }

    private int getLogTerm(int index) {
        if (logs.size() > index && index >= 0) {
            return logs.get(index).getTerm();
        } else {
            return 0;
        }
    }

    private boolean isLatestLog(ElectionRequest request) {
        // 本程序中在start()加了锁，更新logs操作和isLatestLog()互斥
        // 如果投票阶段收到client的命令，logs大小+1, 需要保证handleElectionRequest()中用到logs的地方都是原子的。比如不能
        // 出现request.getLastLogIndex() >= logs.size()是false，但是在此后log发生变化，又将logs.size()发送给了其他节点
        int lastLogIndex = logs.size() - 1;
        int lastLogTerm = getLogTerm(lastLogIndex);
        return request.getLastLogTerm() > lastLogTerm || (request.getLastLogTerm() == lastLogTerm && request.getLastLogIndex() >= lastLogIndex);
    }

    public void handleElectionResponse(ElectionResponse response) {
        // 得加锁
        // 1. 收到leader节点请求刚把term改大，然后这里执行到term = requesterTerm，导致term被错误更新
        // 2. 重新成为candidate时候会更新term
        lock.lock();
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
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1,
            new BasicThreadFactory.Builder()
                .namingPattern("Leader-pool-")
                .uncaughtExceptionHandler((t, e) -> {
                        log.error(e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                ).build()
        );
        lock.lock();
        for (String peer : peers.keySet()) {
            if (!peer.equals(me)) {
                nextIndexMap.put(peer, logs.size());
            }
        }
        lock.unlock();
        Runnable broadCastHeartBeat = new Runnable() {
            @Override
            public void run() {
                lock.lock();
                if (Leader.equals(role) && !stop) {
                    AppendEntriesRequest heartBeatRequest = new AppendEntriesRequest();
                    heartBeatRequest.setTracingId();
                    heartBeatRequest.setLeaderId(me);
                    heartBeatRequest.setTerm(term);
                    heartBeatRequest.setLeaderCommit(commitIndex);
                    for (Map.Entry<String, RpcClient> peer : peers.entrySet()) {
                        String peerId = peer.getKey();
                        if (me.equals(peerId)) {
                            continue;
                        }
                        // 收到response时候需要更新nextIndexMap，注意线程安全
                        int nextIndex = nextIndexMap.get(peerId);
                        int prevLogIndex = nextIndex - 1;
                        int lastIndex = Math.min(logs.size() - 1, nextIndex + ClusterConfigConstant.MAX_BATCH_SIZE);
                        int prevLogTerm = prevLogIndex >= 0 && logs.size() > 0 ? logs.get(prevLogIndex).getTerm() : 0;
                        // 初始化时nextIndex == logs.size()，此时发空心跳包检测match情况，不match再减少nextIndex
                        // 异步调用RPC接口，传入参数中包含list接口，与Raft其他线程并发修改list会造成ConcurrentModificationException
                        List<LogEntry> logEntries = (nextIndex == logs.size() || logs.size() == 0) ? new ArrayList<>() : new ArrayList<>(logs.subList(nextIndex, lastIndex + 1));
                        if (logEntries.size() == 0) {
                            log.info("empty entry nextIndex-{} lastIndex-{}", nextIndex, lastIndex);
                        } else {
                            log.info("not-empty entry nextIndex-{} lastIndex-{}", nextIndex, lastIndex);
                        }
                        heartBeatRequest.setPrevLogIndex(prevLogIndex);
                        heartBeatRequest.setPreLogTerm(prevLogTerm);
                        heartBeatRequest.setEntries(logEntries);
                        heartBeatRequest.setLeaderCommit(commitIndex);
                        rpcCallAsync(peerId, "handleHeartBeatRequest", heartBeatRequest);
                    }
                    scheduler.schedule(this, ClusterConfigConstant.HEART_BEAT_TIMEOUT, TimeUnit.MILLISECONDS);
                } else {
                    log.info("{} stop!!!", me);
                    scheduler.shutdown();
                }
                lock.unlock();
            }
        };
        scheduler.schedule(broadCastHeartBeat, ClusterConfigConstant.HEART_BEAT_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    public void handleHeartBeatRequest(AppendEntriesRequest heartBeatRequest) {
        lock.lock();
        AppendEntriesResponse appendEntriesResponse = new AppendEntriesResponse();
        appendEntriesResponse.setNodeId(me);
        appendEntriesResponse.setRequesterTerm(heartBeatRequest.getTerm());
        if (heartBeatRequest.getTerm() < term) {
            appendEntriesResponse.setResponderTerm(term);
            appendEntriesResponse.setSuccess(false);
            rpcCallAsync(heartBeatRequest.getLeaderId(), "handleHeartBeatResponse", appendEntriesResponse);
        } else {
            term = heartBeatRequest.getTerm();
            appendEntriesResponse.setResponderTerm(term);
            int prevLogIndex = heartBeatRequest.getPrevLogIndex();
            if (prevLogIndex == -1 || (prevLogIndex < logs.size() && heartBeatRequest.getPreLogTerm() == logs.get(prevLogIndex).getTerm())) {
                appendEntriesResponse.setSuccess(true);
                if (logs.size() - 1 == prevLogIndex) {
                    logs.addAll(heartBeatRequest.getEntries());
                } else {
                    // 删除可能产生冲突的日志
                    logs = logs.subList(0, prevLogIndex + 1);
                    logs.addAll(heartBeatRequest.getEntries());
                }
                appendEntriesResponse.setMatchIndex(logs.size() - 1);
                // 可能还没和leader节点日志完全同步Math.min(heartBeatRequest.getLeaderCommit(), logs.size() - 1)
                log.info("node {}, term {}, commitIndex {}, leader request {}, logs last index {} ", me, term, commitIndex, heartBeatRequest, logs.size() - 1);
                commitIndex = Math.max(commitIndex, Math.min(heartBeatRequest.getLeaderCommit(), logs.size() - 1));
                log.info("node {}, term {}, commitIndex {}, lastApplied {} ", me, term, commitIndex, lastApplied);
                replyToClient();
            } else {
                appendEntriesResponse.setSuccess(false);
            }
            rpcCallAsync(heartBeatRequest.getLeaderId(), "handleHeartBeatResponse", appendEntriesResponse);
            doFollower();
        }
        lock.unlock();
    }

    public void handleHeartBeatResponse(AppendEntriesResponse appendEntriesResponse) {
        lock.lock();
        // 可能是去年作为leader时候发出的AppendEntriesRequest的response
        // 忽略掉不是自己任期时的消息
        if (appendEntriesResponse.getRequesterTerm() != term) {
            lock.unlock();
            return;
        }
        int peerTerm = appendEntriesResponse.getResponderTerm();
        if (peerTerm > term) {
            term = appendEntriesResponse.getResponderTerm();
            doFollower();
            lock.unlock();
            return;
        }
        String peer = appendEntriesResponse.getNodeId();
        if (appendEntriesResponse.isSuccess()) {
            int matchIndex = Math.max(matchIndexMap.get(peer), appendEntriesResponse.getMatchIndex());
            matchIndexMap.put(peer, matchIndex);
            nextIndexMap.put(peer, matchIndex + 1);
            updateCommitIndex(matchIndex);
            replyToClient();
        } else {
            // 每次nextIndex减1效率可能不高,让nextIndex回退多点
            int nextIndex = Math.max(matchIndexMap.get(peer) + 1, Math.max(0, nextIndexMap.get(peer) - ClusterConfigConstant.MAX_BATCH_SIZE));
            nextIndexMap.put(peer, nextIndex);
        }
        lock.unlock();
    }

    private void updateCommitIndex(int matchIndex) {
        // Leader节点本身有日志了，但是matchIndex还是-1，之前的heartBeatResponse在有了日志后才收到
        if (logs.size() == 0 || matchIndex < 0) {
            return;
        }
        int count = 0;
        for (int peerMatchIndex : matchIndexMap.values()) {
            if (peerMatchIndex >= matchIndex) {
                ++count;
            }
        }
        // logs.get(matchIndex).getTerm() == term leader节点只能提交自己term的日志
        if (count >= peers.size() / 2 && logs.get(matchIndex).getTerm() == term) {
            commitIndex = Math.max(commitIndex, matchIndex);
        }
    }

    /**
     * 将未提交的日志作为响应返回给客户端
     * testBasicAgree2B开始就需要完成这个函数
     * 测试中检查各个节点的logs需要使用这个函数
     * 具体参考applyCh在测试代码ClusterConfig中的使用
     */
    private void replyToClient() {
        if (commitIndex >= 0) {
            log.info("node {} term {} commitIndex {} lastApplied {}", me, term, commitIndex, lastApplied);
        }
        for (int i = lastApplied + 1; i <= commitIndex; ++i) {
            ApplyMsg applyMsg = ApplyMsg.builder()
                .commandValid(true)
                .commandIndex(i)
                .command(logs.get(i).getCommand())
                .build();
            applyCh.offer(applyMsg, 2 * 1000, TimeUnit.MILLISECONDS);
        }
        log.info("node {} term {} commitIndex {} lastApplied {} +1", me, term, commitIndex, lastApplied);
        lastApplied = commitIndex;
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
            log.info("me {} call other peer {} {}, my state {}", me, peerName, args, getState());
            RpcClient peer = entry.getValue();
            Runnable runnable = () -> peer.call(methodName, args);
            // 可以通过其他手段控制线程池最长执行时间，避免线程太多。
            new Thread(runnable, "call-peer-thread").start();
        }
    }

    public void rpcCallAsync(String nodeId, String methodName, Object... args) {
        log.info("me {} call other peer {} {}, my state {}", me, args, nodeId, getState());
        new Thread(() -> peers.get(nodeId).call(methodName, args)).start();
    }

    public void kill() {
        stop = true;
    }
}
