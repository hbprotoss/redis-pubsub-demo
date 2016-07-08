package io.hbprotoss.redis.sub;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

/**
 * Created by hbprotoss on 7/8/16.
 */
public class Subscriber extends JedisPubSub {

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
        System.out.println(String.format("sub channel:%s", channel));
    }

    @Override
    public void onMessage(String channel, String message) {
        System.out.println(String.format("channel:%s, message:%s", channel, message));
    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {
        System.out.println(String.format("unsub channel:%s", channel));
    }

    public static void main(String[] args) throws InterruptedException {
        Subscriber subscriber = new Subscriber();
        Jedis jedis = new Jedis("192.168.2.19", 6979);
        jedis.subscribe(subscriber, "test-chan");
        Thread.sleep(1000L*1000);
    }
}
