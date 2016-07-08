package io.hbprotoss.redis.pub.task;

import redis.clients.jedis.JedisPool;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * <p class="detail">
 * 功能: 生产者实现
 * </p>
 *
 * @author hbprotoss
 * @ClassName Producer.
 * @Version V1.0.
 * @date 2016.07.08 15:46:30
 */
public class Producer {
    private LinkedBlockingDeque<Object> queue;

    private JedisPool pool;

    private String channel;

    public Producer(LinkedBlockingDeque<Object> queue, JedisPool pool, String channel) {
        this.queue = queue;
        this.pool = pool;
        this.channel = channel;
    }

    public void produce(Object object) throws InterruptedException {
        queue.put(object);
    }
}
