package org.zrj.rpc;

import java.util.concurrent.atomic.AtomicInteger;

public class Metrics {
    public final static String RPC_REPLY_OFFER_COUNT_METRICS = "RPC_REPLY_OFFER_COUNT";
    public final static String RPC_REPLY_POLL_COUNT_METRICS = "RPC_REPLY_POLL_COUNT";
    public final static String RPC_TOTAL_REQUEST_COUNT_METRICS = "RPC_TOTAL_REQUEST_COUNT";

    // network先将reply从rpc channel获取出来，然后发送给request.replyChannel
    public final static AtomicInteger RPC_REPLY_OFFER_COUNT = new AtomicInteger();
    // rpc client从request.replyChannel取出数据
    public final static AtomicInteger RPC_REPLY_POLL_COUNT = new AtomicInteger();
    public final static AtomicInteger RPC_TOTAL_REQUEST_COUNT = new AtomicInteger();
}
