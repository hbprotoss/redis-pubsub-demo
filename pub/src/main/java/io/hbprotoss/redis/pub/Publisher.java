package io.hbprotoss.redis.pub;

import io.hbprotoss.redis.pub.task.TaskManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Created by hbprotoss on 7/8/16.
 */
public class Publisher {
    public static void main(String[] args) throws InterruptedException {
        TaskManager taskManager = new TaskManager(new JedisPool("192.168.2.19", 6979), "test-chan");
        for (int i = 0; i < 1000; i++) {
            String msg = String.format("message %d", i);
            taskManager.publish(msg);
            System.out.println(msg);
            Thread.sleep(1000L);
        }
    }
}
