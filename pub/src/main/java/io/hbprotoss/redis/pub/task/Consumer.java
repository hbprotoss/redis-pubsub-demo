package io.hbprotoss.redis.pub.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;


/**
 * <p class="detail">
 * 功能: 消费者实现
 * 注意, 队列满之后未发送的消息会被丢弃
 * </p>
 *
 * @author hbprotoss
 * @ClassName Consumer.
 * @Version V1.0.
 * @date 2016.07.08 15:45:22
 */
public class Consumer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    private LinkedBlockingDeque<Object> queue;

    private JedisPool pool;

    private String channel;

    private static final long RETRY_INTERNAL = 1000L;    // in ms

    public Consumer(LinkedBlockingDeque<Object> queue, JedisPool pool, String channel) {
        this.queue = queue;
        this.pool = pool;
        this.channel = channel;
    }

    @Override
    public void run() {
        while (true) {
            Object object;
            try {
                object = queue.pollFirst(RETRY_INTERNAL, TimeUnit.MILLISECONDS);
                if (object == null) {
                    logger.debug("pollFirst timeout, retrying...");
                    continue;
                }
            } catch (InterruptedException e) {
                logger.error("interrupted while waiting to queue.take()");
                sleep(RETRY_INTERNAL);
                continue;
            }

            Jedis jedis;
            try {
                jedis = pool.getResource();
                jedis.publish(channel, object.toString());
                logger.debug("message({}) sent to channel({}) successfully", object.toString(), channel);
            } catch (Exception e) {
                logger.error("error when jedis.publish({}, {})", channel, object.toString(), e);
                try {
                    // 尝试加回队列重试
                    logger.debug("retry jedis.publish({}, {})", channel, object.toString());
                    if (!queue.offerFirst(object, RETRY_INTERNAL, TimeUnit.MILLISECONDS)) {
                        logger.error("queue is full, lost message({})", object.toString());
                    }
                } catch (InterruptedException e1) {
                    logger.error("interrupted while waiting to queue.putFirst({})", object);
                }
                sleep(RETRY_INTERNAL);
                continue;
            }
            jedis.close();
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e1) {
            logger.error("interrupted while waiting to retry");
        }
    }
}
