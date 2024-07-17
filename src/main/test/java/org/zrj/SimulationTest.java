package org.zrj;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.zrj.raft.ClusterConfig;
import org.zrj.raft.Sleep;

@Slf4j
public class SimulationTest {
    private final int RaftElectionTimeout = 1000;
    static int a = 0;

    @Test
    public void test() {
        for(int i = 0; i < 100; ++i) {
            a = i;
            testInitialElection2A();
            Sleep.sleep(100);
        }
    }

    @Test
    public static void main(String[] args) {
        SimulationTest simulationTest = new SimulationTest();
        for(int i = 0; i < 50; ++i) {
            a = i;
            simulationTest.testInitialElection2A();
        }
    }

    @Test
    public void testInitialElection2A() {
        int size = 3;
        ClusterConfig clusterConfig = new ClusterConfig(size, false, a);
        clusterConfig.begin("Test (2A): initial election");

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

        clusterConfig.end();
        clusterConfig.cleanUp();
    }

    @Test
    public void testReElection2A() {

    }
}
