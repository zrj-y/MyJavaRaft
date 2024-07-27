package org.zrj;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.zrj.raft.ClusterConfig;
import org.zrj.raft.Sleep;
import org.zrj.raft.StartResponse;

import java.util.List;

@Slf4j
public class SimulationTest {
    private final int RaftElectionTimeout = 1000;
    private static int testId = 0;
    public static void main(String[] args) {
        SimulationTest simulationTest = new SimulationTest();
        for (int i = 0; i < 10; ++i) {
            testId = i;
            simulationTest.testFailNoAgree2B();
        }
    }

    // 设置全局的未捕获异常处理器
    @Before
    public void setup() {
        Thread.setDefaultUncaughtExceptionHandler((thread, throwable) -> {
            log.error(throwable.getMessage(), throwable);
        });
    }

    @Test
    public void testInitialElection2A() {
        int size = 3;
        ClusterConfig clusterConfig = new ClusterConfig(size, false, testId);
        clusterConfig.begin("Test (2A): initial election");
        try {
            // is a leader elected?
            clusterConfig.checkOneLeader();

            // sleep a bit to avoid racing with followers learning of the
            // election, then check that all peers agree on the term.
            Sleep.sleep(50);
            int term1 = clusterConfig.checkTerms();
            if (term1 < 1) {
                throw new RuntimeException(String.format("term is %s, but should be at least 1", term1));
            }

            // does the leader+term stay the same if there is no network failure?
            Sleep.sleep(2 * RaftElectionTimeout);
            int term2 = clusterConfig.checkTerms();
            if (term1 != term2) {
                log.warn("warning: term changed even though there were no failures");
            }

            // there should still be a leader.
            clusterConfig.checkOneLeader();
        } finally {
            clusterConfig.end();
            clusterConfig.cleanUp();
        }
    }

    @Test
    public void testReElection2A() {
        int servers = 3;
        ClusterConfig clusterConfig = new ClusterConfig(servers, false, testId);
        clusterConfig.begin("Test (2A): election after network failure");

        String leader1 = clusterConfig.checkOneLeader();

        // if the leader disconnects, a new one should be elected.
        clusterConfig.disconnect(leader1);
        clusterConfig.checkOneLeader();

        // if the old leader rejoins, that shouldn't
        // disturb the new leader.
        clusterConfig.connect(leader1);
        String leader2 = clusterConfig.checkOneLeader();

        // if there's no quorum, no leader should
        // be elected.
        clusterConfig.disconnect(leader2);
        clusterConfig.disconnect(clusterConfig.nextNode(leader2));
        Sleep.sleep(2 * RaftElectionTimeout);
        clusterConfig.checkNoLeader();

        // if a quorum arises, it should elect a leader.
        clusterConfig.connect(clusterConfig.nextNode(leader2));
        clusterConfig.checkOneLeader();

        // re-join of last node shouldn't prevent leader from existing.
        clusterConfig.connect(leader2);
        clusterConfig.checkOneLeader();

        clusterConfig.end();
        clusterConfig.cleanUp();
    }

    @Test
    public void testBasicAgree2B() {
        int servers = 3;
        ClusterConfig clusterConfig = new ClusterConfig(servers, false, testId);
        clusterConfig.begin("Test (2B): basic agreement");

        int iters = 3;
        // 论文中log下标从1开始，
        // 因此MIT课程代码是int index = 1; index < iters + 1; ++index，如果按照这种方式写测试代码，对应的Raft代码也需要修改，可以在初始化Raft节点时候
        // 往log加一个dummy的log entry，或者修改Raft代码里nextIndex等初始化的数值
        for (int index = 0; index < iters; ++index) {
            int count = clusterConfig.nCommitted(index).getCount();
            if (count > 0) {
                throw new RuntimeException("some have committed before Start()");
            }
            int xindex = clusterConfig.one(String.valueOf(index * 100), servers, false);
            if (xindex != index) {
                throw new RuntimeException(String.format("got index %d but expected %d", xindex, index));
            }
        }

        clusterConfig.end();
        clusterConfig.cleanUp();
    }

    // 试了用Java工具获取对象大小性能太差，需要几十毫秒，因此这个测试不完全和原课程一致
    // check, based on counting bytes of RPCs, that
    // each command is sent to each peer just once.
    @Test
    public void testRPCBytes2B() {
        int servers = 3;
        ClusterConfig clusterConfig = new ClusterConfig(servers, false, testId);
        clusterConfig.begin("Test (2B): RPC byte count");

        clusterConfig.one("99", servers, false);
        long bytes0 = clusterConfig.bytesTotal();

        int iters = 10;
        long sent = 0;
        int cmdSize = 500;
        for (int index = 1; index <= iters; index++) {
            String cmd = RandomStringUtils.randomAlphabetic(cmdSize);
            int xindex = clusterConfig.one(cmd, servers, false);
            if (xindex != index) {
                throw new RuntimeException(String.format("got index %d but expected %d", xindex, index));
            }
            sent += cmd.length();
        }

        long bytes1 = clusterConfig.bytesTotal();
        long got = bytes1 - bytes0;
        long expected = (servers - 1) * sent;
        if (got > expected) {
            throw new RuntimeException(String.format("too many RPC bytes; got %d, expected %d", got, expected));
        }
        clusterConfig.end();
        clusterConfig.cleanUp();
    }

    @Test
    public void testFailAgree2B() {
        int servers = 3;
        ClusterConfig cfg = new ClusterConfig(servers, false, testId);
        cfg.begin("Test (2B): agreement despite follower disconnection");

        cfg.one("101", servers, false);

        // disconnect one follower from the network.
        String leader = cfg.checkOneLeader();
        cfg.disconnect(cfg.nextNode(leader));

        // the leader and remaining follower should be
        // able to agree despite the disconnected follower.
        cfg.one("102", servers - 1, false);
        cfg.one("103", servers - 1, false);
        Sleep.sleep(RaftElectionTimeout);
        cfg.one("104", servers - 1, false);
        cfg.one("105", servers - 1, false);

        // re-connect
        cfg.connect(cfg.nextNode(leader));

        // the full set of servers should preserve
        // previous agreements, and be able to agree
        // on new commands.
        cfg.one("106", servers, true);
        Sleep.sleep(RaftElectionTimeout);
        cfg.one("107", servers, true);

        cfg.end();
        cfg.cleanUp();
    }

    @Test
    public void testFailNoAgree2B() {
        int servers = 5;
        ClusterConfig cfg = new ClusterConfig(servers, false, testId);

        cfg.begin("Test (2B): no agreement if too many followers disconnect");

        cfg.one("10", servers, false);

        // 3 of 5 followers disconnect
        String leader = cfg.checkOneLeader();
        // next1 next2 next3不可能和leader是同一个节点
        String next1 = cfg.nextNode(leader);
        String next2 = cfg.nextNode(next1);
        String next3 = cfg.nextNode(next2);
        cfg.disconnect(next1);
        cfg.disconnect(next2);
        cfg.disconnect(next3);

        StartResponse response = cfg.getNode(leader).start("20");
        if (!response.isLeader()) {
            throw new RuntimeException("leader rejected Start()");
        }
        int index = response.getCommandIndex();
        if (index != 1) {
            throw new RuntimeException(String.format("expected index 1, got %d", index));
        }

        Sleep.sleep(2 * RaftElectionTimeout);

        int n = cfg.nCommitted(index).getCount();
        if (n > 0) {
            throw new RuntimeException(String.format("%d committed but no majority", n));
        }

        // repair
        cfg.connect(next1);
        cfg.connect(next2);
        cfg.connect(next3);

        // the disconnected majority may have chosen a leader from
        // among their own ranks, forgetting index 2.
        String leader2 = cfg.checkOneLeader();
        response = cfg.getNode(leader2).start("30");
        int index2 = response.getCommandIndex();
        if (!response.isLeader()) {
            throw new RuntimeException("leader2 rejected Start()");
        }
        if (index2 < 1 || index2 > 2) {
            throw new RuntimeException(String.format("unexpected index %d", index2));
        }

        cfg.one("1000", servers, true);

        cfg.end();
        cfg.cleanUp();
    }

    @Test
    public void testRejoin2B() {
        int servers = 3;
        ClusterConfig cfg = new ClusterConfig(servers, false, testId);

        cfg.begin("Test (2B): rejoin of partitioned leader");

        cfg.one("101", servers, true);

        // leader network failure
        String leader1 = cfg.checkOneLeader();
        cfg.disconnect(leader1);

        // make old leader try to agree on some entries
        cfg.getNode(leader1).start("102");
        cfg.getNode(leader1).start("103");
        cfg.getNode(leader1).start("104");

        // new leader commits, also for index=2
        cfg.one("103", 2, true);

        // new leader network failure
        String leader2 = cfg.checkOneLeader();
        cfg.disconnect(leader2);

        // old leader connected again
        cfg.connect(leader1);

        cfg.one("104", 2, true);

        // all together now
        cfg.connect(leader2);

        cfg.one("105", servers, true);

        cfg.end();
        cfg.cleanUp();
    }

    @Test
    public void testBackup2B() {
        int servers = 5;
        ClusterConfig cfg = new ClusterConfig(servers, false, testId);

        cfg.begin("Test (2B): leader backs up quickly over incorrect follower logs");
        List<String> nodes = cfg.getNodes();

        cfg.one(RandomStringUtils.randomAlphabetic(5), servers, true);

        // put leader and one follower in a partition
        String leader1 = cfg.checkOneLeader();
        int leader1Id = indexOf(leader1, nodes);
        cfg.disconnect(nodes.get((leader1Id + 2) % servers));
        cfg.disconnect(nodes.get((leader1Id + 3) % servers));
        cfg.disconnect(nodes.get((leader1Id + 4) % servers));

        // submit lots of commands that won't commit
        for (int i = 0; i < 50; i++) {
            cfg.getNode(leader1).start(RandomStringUtils.randomAlphabetic(5));
        }

        Sleep.sleep(RaftElectionTimeout / 2);

        cfg.disconnect(nodes.get((leader1Id) % servers));
        cfg.disconnect(nodes.get((leader1Id + 1) % servers));

        // allow other partition to recover
        cfg.connect(nodes.get((leader1Id + 2) % servers));
        cfg.connect(nodes.get((leader1Id + 3) % servers));
        cfg.connect(nodes.get((leader1Id + 4) % servers));

        // lots of successful commands to new group.
        for (int i = 0; i < 50; i++) {
            cfg.one(RandomStringUtils.randomAlphabetic(5), 3, true);
        }

        // now another partitioned leader and one follower
        String leader2 = cfg.checkOneLeader();
        String other = nodes.get((leader1Id + 2) % servers);
        if (leader2.equals(other)) {
            other = nodes.get((indexOf(leader2, nodes) + 1) % servers);
        }
        cfg.disconnect(other);

        // lots more commands that won't commit
        for (int i = 0; i < 50; i++) {
            cfg.getNode(leader2).start(RandomStringUtils.randomAlphabetic(5));
        }

        Sleep.sleep(RaftElectionTimeout / 2);

        // bring original leader back to life,
        for (String n : cfg.getNodes()) {
            cfg.disconnect(n);
        }
        cfg.connect(leader1);
        cfg.connect(nodes.get((leader1Id + 1) % servers));
        cfg.connect(other);

        // lots of successful commands to new group.
        for (int i = 0; i < 50; i++) {
            cfg.one(RandomStringUtils.randomAlphabetic(5), 3, true);
        }

        // now everyone
        for (String n : cfg.getNodes()) {
            cfg.connect(n);
        }
        cfg.one(RandomStringUtils.randomAlphabetic(5), servers, true);

        cfg.end();
        cfg.cleanUp();
    }

    private int indexOf(String item, List<String> items) {
        for (int i = 0; i < items.size(); ++i) {
            if (items.get(i).equals(item)) {
                return i;
            }
        }
        return -1;
    }
}
