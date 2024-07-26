package org.zrj;

import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.zrj.raft.ClusterConfig;
import org.zrj.raft.Sleep;

@Slf4j
public class SimulationTest {
    private final int RaftElectionTimeout = 1000;
    static int a = 0;

    @Test
    public void test() {
        for (int i = 0; i < 100; ++i) {
            a = i;
            testInitialElection2A();
            Sleep.sleep(100);
        }
    }

    public static void main(String[] args) {
        SimulationTest simulationTest = new SimulationTest();
        for (int i = 0; i < 20; ++i) {
            a = i;
            simulationTest.testBasicAgree2B();
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
        ClusterConfig clusterConfig = new ClusterConfig(size, false, a);
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
        ClusterConfig clusterConfig = new ClusterConfig(servers, false, a);
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
        ClusterConfig clusterConfig = new ClusterConfig(servers, false, a);
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
}
