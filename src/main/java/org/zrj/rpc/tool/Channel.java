package org.zrj.rpc.tool;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

public class Channel<E> extends SynchronousQueue<E> {
    private final SynchronousQueue<E> queue;

    public Channel() {
        this.queue = new SynchronousQueue<>();
    }

    @Override
    public E take() {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(E e) {
        try {
            queue.put(e);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) {
        try {
            return queue.offer(e, timeout, unit);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public E poll(long timeout, TimeUnit unit) {
        return queue.poll();
    }

}
