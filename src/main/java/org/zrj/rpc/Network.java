package org.zrj.rpc;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.zrj.raft.Sleep;
import org.zrj.rpc.tool.Channel;
import org.zrj.rpc.tool.ObjectSize;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Getter
@Slf4j
public class Network {
    private final Lock lock;
    private final Random random;
    private boolean reliable;

    // pause a long time on send on disabled connection
    private boolean longDelays;
    // sometimes delay replies a long time
    private boolean longReordering;
    private final Channel<RpcRequestMessage> endCh;
    private final Done done;
    private final AtomicInteger count;
    private final AtomicLong bytes;

    /**
     * 下面注释用endName表示每个connect
     */

    /**
     * endName to nodeId of callee, endName映射到被调用方
     * 每个pair一个connect, pair有节点重启后连接更换port等，视为新connect
     */
    private final Map<String, String> connections;

    /**
     * endName string to boolean
     * from到to之间的connect是否连通
     */
    private final Map<String, Boolean> enabled;

    /**
     * endName string to RpcClient
     * 日常项目中RpcClient可能与Node一一对应，每个Node内部只引用一个RpcClient,但是每个Client都会有其他Node的配置
     * 所以这里RpcClient与每个connect对应也是符合实际的
     */
    private final Map<String, RpcClient> ends;

    /**
     * nodeId to Raft service
     * 相当于微服务注册中心
     * A调用B则根据A,B之间的end name和connections map获得被调用节点的node id,再根据nodeId和servers map获得被调用节点的服务
     */
    private final Map<String, Server> servers;

    public Network() {
        this.lock = new ReentrantLock();
        this.random = new Random();
        this.reliable = true;
        this.ends = new HashMap<>();
        this.enabled = new HashMap<>();
        this.servers = new HashMap<>();
        this.connections = new HashMap<>();
        this.endCh = new Channel<>();
        this.done = Done.builder().done(false).build();
        this.count = new AtomicInteger(0);
        this.bytes = new AtomicLong(0);
        Thread networkInitThread = new Thread("NetworkInit-pool-") {
            @Override
            public void run() {
                ExecutorService processReqPool = new ThreadPoolExecutor(
                    100, 100, 2, TimeUnit.MINUTES,
                    new LinkedBlockingQueue<>(100), new BasicThreadFactory.Builder().namingPattern("Network-Process-Req-pool-").build(),
                    (r, executor) -> log.warn("Net pool is full, reject the request {}", r)
                );
                while (!done.isDone()) {
                    RpcRequestMessage req = endCh.poll(2 * 1000, TimeUnit.MILLISECONDS);
                    if (req == null) {
                        continue;
                    }
                    count.incrementAndGet();
                    bytes.addAndGet(ObjectSize.bytes(req.getArgs()));
                    log.info("network start process requests num {}, request hashcode {}", count.get(), req.hashCode());
                    processReqPool.execute(() -> processReq(req));
                }
                processReqPool.shutdown();
            }
        };
        networkInitThread.start();
    }

    public void cleanUp() {
        this.done.setDone(true);
    }

    public void setReliable(boolean reliable) {
        doWithLock(() -> this.reliable = reliable);
    }

    public void setLongReordering(boolean longReordering) {
        doWithLock(() -> this.longReordering = longReordering);
    }

    public void setLongDelays(boolean longDelays) {
        doWithLock(() -> this.longDelays = longDelays);
    }

    public boolean isServerDead(String endName, String serverName, Server server) {
        try {
            lock.lock();
            // Todo: servers.get(serverName) != rpcClient 应该就行，不用调用equal
            if (!enabled.get(endName) || servers.get(serverName) != server) {
                return true;
            }
        } finally {
            lock.unlock();
        }
        return false;
    }

    /**
     * 1. 网络不稳定
     * 2. 发送方crash或者网络断开无法发送请求
     * 3. 接收方收到请求，但未执行
     * 2.与3.结果相同,接收方收到的都是空响应，并且接收方没有执行请求
     * 4. 接收方执行，但未发送response (尽管在实现Raft时候请求方基本并不会同步等待response, response对请求方没有意义)
     * 5. 接收方执行，发送了response
     */
    private void processReq(RpcRequestMessage request) {
        String endName = request.getEndName();
        lock.lock();
        boolean enable = enabled.get(endName);
        String serverName = connections.get(endName);
        Server server = Optional.ofNullable(serverName)
            .map(servers::get).orElse(null);
        lock.unlock();
        if (enable && serverName != null && server != null) {
            if (!reliable) {
                Sleep.sleep(random.nextInt(27));
                if (random.nextInt(1000) < 100) {
                    offerReplyToRequestReplyChannel(request, RpcReplyMessage.builder().ok(false).reply(null).build());
                    return;
                }
            }

            Channel<RpcReplyMessage> ech = new Channel<>();

            // 不要阻塞main线程，以便可以检测server
            Thread thread = new Thread("Network-Process-Req-thread-") {
                @Override
                public void run() {
                    RpcReplyMessage reply = server.dispatch(request);
                    boolean replySuccess = ech.offer(reply, 10 * 1000, TimeUnit.MILLISECONDS);
                    if (!replySuccess) {
                        log.warn("{} request has not been replied", request);
                    } else {
                        log.debug("Network process request success {}", request);
                    }
                }
            };
            thread.start();
            RpcReplyMessage reply = null;
            boolean replyOK = false, serverDead = false;
            while (!replyOK && !serverDead) {
                reply = ech.poll(100, TimeUnit.MILLISECONDS);
                if (reply != null) {
                    replyOK = true;
                } else {
                    serverDead = isServerDead(request.getEndName(), serverName, server);
                    if (serverDead) {
                        new Thread(() -> ech.poll(10, TimeUnit.MILLISECONDS), "Consume-last-message-thread-").start();
                    }
                }
            }

            serverDead = isServerDead(request.getEndName(), serverName, server);
            if (!replyOK || serverDead) {
                offerReplyToRequestReplyChannel(request, RpcReplyMessage.builder().ok(false).reply(null).build());
            } else if (!reliable && random.nextInt(1000) < 100) {
                offerReplyToRequestReplyChannel(request, RpcReplyMessage.builder().ok(false).reply(null).build());
            } else if (longReordering && random.nextInt(900) < 600) {
                ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
                    new BasicThreadFactory.Builder().namingPattern("Network-Process-Req-Scheduled-pool1-").build()
                );
                RpcReplyMessage finalReply = reply;
                executor.schedule(() -> {
                    offerReplyToRequestReplyChannel(request, finalReply);
                    bytes.addAndGet(ObjectSize.bytes(finalReply.getReply()));
                }, 200 + random.nextInt(1 + random.nextInt(2000)), TimeUnit.MILLISECONDS);
                executor.shutdown();
            } else {
                offerReplyToRequestReplyChannel(request, reply);
                bytes.addAndGet(ObjectSize.bytes(reply.getReply()));
            }
        } else {
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
                new BasicThreadFactory.Builder().namingPattern("Network-Process-Req-Scheduled-pool2-").build()
            );
            int ms = longDelays ? random.nextInt(7000) : random.nextInt(100);
            executor.schedule(
                () -> {
                    offerReplyToRequestReplyChannel(request, RpcReplyMessage.builder().ok(false).reply(null).build());
                    executor.shutdown();
                },
                ms, TimeUnit.MILLISECONDS
            );
        }
    }

    /**
     * @param name    调用方的name
     * @param endName 调用方和被调用方连接的name
     * @return
     */
    public RpcClient makeEnd(String name, String endName) {
        lock.lock();
        if (ends.containsKey(endName)) {
            throw new RuntimeException(String.format("MakeEnd: %s already exists\n", endName));
        }
        RpcClient rpcClient = new RpcClient(endCh, name, endName, done);
        ends.put(endName, rpcClient);
        enabled.put(endName, false);
        connections.remove(endName);
        lock.unlock();
        return rpcClient;
    }

    public void addServer(String serverName, Server server) {
        doWithLock(() -> servers.put(serverName, server));
    }

    public void deleteServer(String serverName) {
        doWithLock(() -> servers.remove(serverName));
    }

    public void connect(String endName, String serverName) {
        doWithLock(() -> connections.put(endName, serverName));
    }

    public void enable(String endName, boolean enable) {
        doWithLock(() -> enabled.put(endName, enable));
    }

    // get a rpcClient's count of incoming RPCs.
    public int getCount(String serverName) {
        try {
            lock.lock();
            Server server = servers.get(serverName);
            if (server != null) {
                return server.getCount();
            }
            return 0;
        } finally {
            lock.unlock();
        }
    }

    public int getTotalCount() {
        return count.get();
    }

    public long getTotalBytes() {
        return bytes.get();
    }

    private void offerReplyToRequestReplyChannel(RpcRequestMessage request, RpcReplyMessage reply) {
        boolean success = request.getReplyCh().offer(reply, 10, TimeUnit.MILLISECONDS);
        if (success) {
            log.info("offer message to reply channel, request hashcode {}, {} {}", request.hashCode(), Metrics.RPC_REPLY_OFFER_COUNT_METRICS, Metrics.RPC_REPLY_OFFER_COUNT.incrementAndGet());
        } else {
            log.warn("fail to offer message to reply channel");
        }
    }

    private void doWithLock(Runnable r) {
        try {
            lock.lock();
            r.run();
        } finally {
            lock.unlock();
        }
    }

    @Builder
    @Getter
    @EqualsAndHashCode
    public static class Pair {
        @NonNull
        private String from;

        @NonNull
        private String to;
    }

    @Builder
    @Getter
    @Setter
    public static class Done {
        private volatile boolean done;
    }
}
