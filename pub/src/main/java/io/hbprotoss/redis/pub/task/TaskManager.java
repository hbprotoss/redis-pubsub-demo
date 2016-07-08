package io.hbprotoss.redis.pub.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * <p class="detail">
 * 功能: 消息发布逻辑封装
 * </p>
 *
 * @author hbprotoss
 * @ClassName Task manager.
 * @Version V1.0.
 * @date 2016.07.08 15:46:45
 */
public class TaskManager {
    private static final int QUEUE_CAPACITY = 256;
    private static final Logger logger = LoggerFactory.getLogger(TaskManager.class);

    private Producer producer;
    private Consumer consumer;

    public TaskManager(JedisPool pool, String channel) {
        LinkedBlockingDeque<Object> queue = new LinkedBlockingDeque<>(QUEUE_CAPACITY);
        producer = new Producer(queue, pool, channel);
        consumer = new Consumer(queue, pool, channel);
        Thread thread = new Thread(consumer);
        thread.setDaemon(false);        // do not lose message
        thread.start();
    }

    public void publish(Object object) throws InterruptedException {
        logger.debug("publishing message({})", object.toString());
        producer.produce(object);
    }
}
