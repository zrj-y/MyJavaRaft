package org.zrj.raft;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.zrj.rpc.Network;
import org.zrj.rpc.RpcClient;
import org.zrj.rpc.Server;
import org.zrj.rpc.tool.Channel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class ClusterConfig {

    private static final String NODE_NAME_PREFIX = "node-%s";
    private final Lock lock;
    private final Network network;
    private final int nodeCount;
    private final List<String> nodeIds;
    private final Map<String, Raft> rafts; // NodeId -> Raft对象
    private final Map<String, String> applyErr; // NodeId -> 错误信息
    private final Map<Network.Pair, String> endNames; // Pair -> end name, 根据Pair包含的两个节点确定它们的连接也就是end name
    private final Map<String, Boolean> connected; // NodeId -> boolean, Node是否连入Net
    private final Map<String, Persister> saved; // NodeId -> Persister, 每个节点已保存的文件
    private final Map<String, List<String>> logs; // NodeId -> boolean, 每个节点的日志
    private final long start;
    private int maxIndex;
    private long t0;
    private int rpcs0; // rpcTotal() at start of test
    private int cmds0; // number of agreements
    private int maxIndex0;
    private long bytes0;
    private final Random random;

    private volatile boolean end;

    public ClusterConfig(int n, boolean unreliable, int test) {
        this.lock = new ReentrantLock();
        this.random = new Random();
        this.network = new Network();
        this.nodeCount = n;
        this.applyErr = new HashMap<>();
        this.rafts = new HashMap<>();
        this.nodeIds = new ArrayList<>();
        this.connected = new HashMap<>();
        this.saved = new HashMap<>();
        this.endNames = new HashMap<>();
        this.logs = new HashMap<>();
        this.start = System.currentTimeMillis();
        this.end = false;

        this.setUnreliable(unreliable);
        this.network.setLongDelays(true);

        for (int i = 0; i < nodeCount; ++i) {
            String nodeName = String.format(NODE_NAME_PREFIX, i) + "-tst-" + test;
            nodeIds.add(nodeName);
        }

        // 创建Raft节点
        for (int i = 0; i < nodeCount; ++i) {
            logs.put(nodeIds.get(i), new ArrayList<>());
            start(nodeIds.get(i));
        }

        // 连接所有节点到network
        for (int i = 0; i < nodeCount; ++i) {
            connect(nodeIds.get(i));
        }
    }

    public void crash(String nodeId) {
        disconnect(nodeId);
        network.deleteServer(nodeId);
        lock.lock();
        if (saved.containsKey(nodeId)) {
            saved.put(nodeId, saved.get(nodeId).copy());
        }
        Raft raft = rafts.get(nodeId);
        if (raft != null) {
            lock.unlock();
            raft.kill();
            lock.lock();
            rafts.remove(nodeId);
        }

        // Crash 只保留log,丢失了snapshot等
        if (saved.containsKey(nodeId)) {
            byte[] rafLog = saved.get(nodeId).readRaftState();
            saved.put(nodeId, new Persister());
            saved.get(nodeId).saveRaftState(rafLog);
        }
        lock.unlock();
    }

    /**
     * 开始或重启raft节点
     *
     * @param nodeId
     */
    public void start(String nodeId) {
        log.info("start node {}", nodeId);
        crash(nodeId);
        for (int j = 0; j < nodeCount; ++j) {
            String peerNode = nodeIds.get(j);
            Network.Pair pair = Network.Pair.builder().from(nodeId).to(peerNode).build();
            endNames.put(pair, String.format("%s-%s-%s", UUID.randomUUID().toString().substring(0, 5), pair.getFrom(), pair.getTo()));
        }

        // 这个节点调用其他节点的全新的clientEnds
        Map<String, RpcClient> ends = new HashMap<>();
        for (int j = 0; j < nodeCount; ++j) {
            String peerNode = nodeIds.get(j);
            Network.Pair pair = Network.Pair.builder().from(nodeId).to(peerNode).build();
            ends.put(peerNode, this.network.makeEnd(nodeId, endNames.get(pair)));
            this.network.connect(endNames.get(pair), peerNode);
        }

        this.lock.lock();
        if (this.saved.containsKey(nodeId)) {
            // todo 注意这个copy函数是浅拷贝
            this.saved.put(nodeId, this.saved.get(nodeId).copy());
        } else {
            this.saved.put(nodeId, new Persister());
        }
        this.lock.unlock();

        Channel<ApplyMsg> applyCh = new Channel<>();
        Thread t = new Thread("ClusterConfig-") {

            @Override
            public void run() {
                while (!end) {
                    ApplyMsg m = applyCh.poll(1000 * 10, TimeUnit.MILLISECONDS);
                    if (m == null) {
                        continue;
                    }
                    String errMsg = "";
                    // ignore other types of ApplyMsg
                    if (m.isCommandValid()) {
                        String v = m.getCommand();
                        lock.lock();
                        for (String node : nodeIds) {
                            List<String> nodeLogs = logs.get(node);
                            if (nodeLogs.size() > m.getCommandIndex()) {
                                String old = nodeLogs.get(m.getCommandIndex());
                                if (old != null && !old.equals(v)) {
                                    // some server has already committed a different value for this entry!
                                    errMsg = String.format("commit index=%d server=%s %s != server=%s %s",
                                        m.getCommandIndex(), nodeId, m.getCommand(), node, old);
                                }
                            }
                        }
                        logs.get(nodeId).add(m.getCommandIndex(), v);
                        maxIndex = Math.max(m.getCommandIndex(), maxIndex);

                        if (m.getCommandIndex() > 1 && logs.get(nodeId).get(m.getCommandIndex() - 1) == null) {
                            errMsg = String.format("server %s apply out of order %d", nodeId, m.getCommandIndex());
                        }
                        lock.unlock();
                    }

                    if (!errMsg.isEmpty()) {
                        applyErr.put(nodeId, errMsg);
                        throw new RuntimeException(String.format("apply error: %s%n", errMsg));
                        // keep reading after error so that Raft doesn't block
                        // holding locks...
                    }
                }
            }
        };
        t.start();
        Raft raft = new Raft(ends, nodeId, this.saved.get(nodeId), applyCh);
        this.lock.lock();
        rafts.put(nodeId, raft);
        this.lock.unlock();

        Server server = new Server();
        server.addService(raft);
        this.network.addServer(nodeId, server);
    }

    public void checkTimeout() {
        if (System.currentTimeMillis() - start > 120 * 60 * 1000) {
            throw new RuntimeException("test took longer than 120 seconds");
        }
    }

    public void cleanUp() {
        for (String n : nodeIds) {
            if (rafts.containsKey(n)) {
                rafts.get(n).kill();
            }
        }
        network.cleanUp();
        checkTimeout();
    }

    public void connect(String nodeId) {
        enableConnect(nodeId, true);
    }

    public void disconnect(String nodeId) {
        enableConnect(nodeId, false);
    }

    private void enableConnect(String nodeId, boolean enable) {
        connected.put(nodeId, enable);

        // outgoing ClientEnds
        for (int j = 0; j < nodeCount; ++j) {
            String endName = endNames.get(Network.Pair.builder().from(nodeId).to(nodeIds.get(j)).build());
            network.enable(endName, enable);
        }
        // incoming ClientEnds
        for (int j = 0; j < nodeCount; ++j) {
            String endName = endNames.get(Network.Pair.builder().from(nodeIds.get(j)).to(nodeId).build());
            network.enable(endName, true);
        }
    }

    public int rpcCount(String server) {
        return network.getCount(server);
    }

    public int rpcTotal() {
        return network.getTotalCount();
    }

    public void setUnreliable(boolean unrel) {
        network.setReliable(!unrel);
    }

    public long bytesTotal() {
        return network.getTotalBytes();
    }

    public String checkOneLeader() {
        for (int iters = 0; iters < 10; iters++) {
            long ms = 450 + new Random().nextLong(100);
            Sleep.sleep(ms);

            Map<Integer, List<String>> leaders = new HashMap<>();
            for (int i = 0; i < nodeCount; i++) {
                String node = nodeIds.get(i);
                if (connected.containsKey(node)) {
                    State state = rafts.get(node).getState();
                    if (state.isLeader()) {
                        leaders.computeIfAbsent(state.getTerm(), k -> new ArrayList<>()).add(node);
                    }
                }
            }

            int lastTermWithLeader = -1;
            for (Map.Entry<Integer, List<String>> entry : leaders.entrySet()) {
                int term = entry.getKey();
                List<String> termLeaders = entry.getValue();
                if (termLeaders.size() > 1) {
                    throw new RuntimeException(String.format("term %d has %d (>1) leaders", term, termLeaders.size()));
                }
                lastTermWithLeader = Math.max(term, lastTermWithLeader);
            }

            if (!leaders.isEmpty()) {
                return leaders.get(lastTermWithLeader).get(0);
            }
        }
        throw new RuntimeException("expected one leader, got none");
    }

    /**
     * check that everyone agrees on the term.
     * 注意节点即使connect的也可能没收到leader的消息成为候选者，然后自增term，这时候checkTerms()也会抛出异常
     */
    public int checkTerms() {
        int term = -1;
        for (int i = 0; i < nodeCount; ++i) {
            String node = nodeIds.get(i);
            if (connected.get(node)) {
                int xterm = rafts.get(node).getState().getTerm();
                if (term == -1) {
                    term = xterm;
                } else if (term != xterm) {
                    throw new RuntimeException("servers disagree on term");
                }
            }
        }
        return term;
    }

    public void checkNoLeader() {
        for (int i = 0; i < nodeCount; ++i) {
            String node = nodeIds.get(i);
            if (connected.get(node)) {
                boolean isLeader = rafts.get(node).getState().isLeader();
                if (isLeader) {
                    throw new RuntimeException(String.format("expected no leader, but %s claims to be leader", node));
                }
            }
        }
    }

    // how many servers think a log entry is committed?
    public NCommit nCommitted(int index) {
        int count = 0;
        String cmd = null;
        for (Map.Entry<String, Raft> entry : rafts.entrySet()) {
            String name = entry.getKey();
            if (applyErr.containsKey(name)) {
                throw new RuntimeException(applyErr.get(name));
            }
            lock.lock();
            String cmd1 = logs.get(name).size() > index ? logs.get(name).get(index) : null;
            lock.unlock();
            if (cmd1 != null) {
                if (count > 0 && !cmd.equals(cmd1)) {
                    throw new RuntimeException(String.format("committed values do not match: index %d, %s, %s\n", index, cmd, cmd1));
                }
                count += 1;
                cmd = cmd1;
            }
        }
        return NCommit.builder().cmd(cmd).count(count).build();
    }

    /**
     * wait for at least n servers to commit.
     * but don't wait forever.
     */
    public String wait(int index, int n, int startTerm) {
        long to = 10; // milliseconds
        for (int iters = 0; iters < 30; iters++) {
            int nd = nCommitted(index).getCount();
            if (nd >= n) {
                break;
            }
            Sleep.sleep(to);
            if (to < 1000) {
                to *= 2;
            }
            if (startTerm > -1) {
                for (Raft r : rafts.values()) {
                    int t = r.getState().getTerm();
                    if (t > startTerm) {
                        // someone has moved on
                        // can no longer guarantee that we'll "win"
                        return null;
                    }
                }
            }
        }
        NCommit nd = nCommitted(index);
        if (nd.getCount() < n) {
            throw new RuntimeException(String.format("only %d decided for index %d; wanted %d\n", nd.getCount(), index, n));
        }
        return nd.getCmd();
    }

    /**
     * do a complete agreement.
     * it might choose the wrong leader initially,
     * and have to re-submit after giving up.
     * entirely gives up after about 10 seconds.
     * indirectly checks that the servers agree on the
     * same value, since nCommitted() checks this,
     * as do the threads that read from applyCh.
     * returns index.
     * if retry==true, may submit the command multiple
     * times, in case a leader fails just after Start().
     * if retry==false, calls Start() only once, in order
     * to simplify the early Lab 2B tests.
     */
    public int one(String cmd, int expectedServers, boolean retry) {
        long t0 = System.currentTimeMillis();
        int starts = 0;
        while (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - t0) < 10) {
            // try all the servers, maybe one is the leader.
            int index = -1;
            for (int si = 0; si < nodeCount; ++si) {
                // 不明白为什么要将starts +1
                starts = (starts + 1) % nodeCount;
                String nodeName = nodeIds.get(starts);
                Raft rf = null;
                lock.lock();
                if (connected.get(nodeName)) {
                    rf = rafts.get(nodeName);
                }
                lock.unlock();
                if (rf != null) {
                    StartResponse result = rf.start(cmd);
                    if (result.isLeader()) {
                        index = result.getCommandIndex();
                        break;
                    }
                }
            }

            if (index != -1) {
                /*
                  somebody claimed to be the leader and to have
                  submitted our command; wait a while for agreement.
                 */
                long t1 = System.currentTimeMillis();
                while (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - t1) < 2) {
                    NCommit nd = nCommitted(index);
                    if (nd.getCount() > 0 && nd.getCount() >= expectedServers) {
                        // committed
                        if (nd.getCmd().equals(cmd)) {
                            // and it was the command we submitted.
                            return index;
                        }
                    }
                    Sleep.sleep(20);
                }
                if (!retry) {
                    throw new RuntimeException(String.format("one(%s) failed to reach agreement", cmd));
                }
            } else {
                Sleep.sleep(50);
            }
        }
        throw new RuntimeException(String.format("one(%s) failed to reach agreement", cmd));
    }

    /**
     * start a Test.
     * print the Test message.
     * e.g. cfg.begin("Test (2B): RPC counts aren't too high")
     */
    public void begin(String description) {
        log.info("{} ...\n", description);
        t0 = System.currentTimeMillis();
        rpcs0 = rpcTotal();
        bytes0 = bytesTotal();
        cmds0 = 0;
        maxIndex0 = maxIndex;
    }

    /**
     * end a Test -- the fact that we got here means there
     * was no failure.
     * print the Passed message,
     * and some performance numbers.
     * 如果测试期间出错会throw exception,执行不到end()
     */
    public void end() {
        checkTimeout();
        lock.lock();
        long t = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - t0);       // real time
        int npeers = nodeCount; // number of Raft peers
        int nrpc = rpcTotal() - rpcs0; // number of RPC sends
        long nbytes = bytesTotal() - bytes0; // number of bytes
        int ncmds = maxIndex - maxIndex0;   // number of Raft agreements reported
        end = true;
        lock.unlock();

        log.info("  ... Passed --");
        log.info("  {}  {} {} {} {}}\n", t, npeers, nrpc, nbytes, ncmds);
    }

    public String nextNode(String nodeId) {
        for (int i = 0; i < nodeIds.size(); ++i) {
            if (nodeIds.get(i).equals(nodeId)) {
                return nodeIds.get((i + 1) % nodeIds.size());
            }
        }
        return null;
    }

    @Builder
    @Getter
    @ToString
    public static class NCommit {
        private int count;
        private String cmd;
    }

}
