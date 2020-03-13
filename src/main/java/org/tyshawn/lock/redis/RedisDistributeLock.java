package org.tyshawn.lock.redis;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

/**
 * redis分布式锁
 * @author litianxiang
 * @date 2020/3/13 10:33
 */
public class RedisDistributeLock {
	private static Logger logger = LoggerFactory.getLogger(RedisDistributeLock.class);

	private Jedis jedis;

	public RedisDistributeLock(String host) {
		JedisPool jedisPool = new JedisPool(host, 6379);
		jedis = jedisPool.getResource();
	}

	/**
	 * 获取锁
	 * @param key
	 * @param value
	 * @param expireTime 过期时间, 单位毫秒
	 */
	public void getLock(String key, String value, long expireTime) {
		try {
			SetParams params = new SetParams();
			params.px(expireTime);
			params.nx();
			while (true) {
				// 旧版本的Jedis使用命令: String result = jedis.set(key, value, "NX", "PX", 100);
				String result = jedis.set(key, value, params);
				if ("OK".equals(result)) {
					return;
				}
				Thread.sleep(100L);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 释放锁
	 * @param key
	 * @param value
	 */
	public void releaseLock(String key, String value) {
		String script = "if redis.call('get',KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
		jedis.eval(script, Collections.singletonList(key), Collections.singletonList(value));
	}

	public static void main(String[] args) {
		ExecutorService executorService = Executors.newFixedThreadPool(5);
		for (int i = 0; i < 5; i++) {
			executorService.execute(new Runnable() {
				@Override
				public void run() {
					RedisDistributeLock redisDistributeLock = new RedisDistributeLock("xxx.xx.xx.xxx");

					//获取锁, 没有获取到锁就继续尝试获取锁
					String key = "my_lock";
					String value = UUID.randomUUID().toString();
					redisDistributeLock.getLock(key, value, 200L);
					try {
						logger.info(Thread.currentThread().getName() + " 进行扣减库存操作...");
					} catch (Exception e) {
						logger.error("处理业务逻辑报错", e);
					}finally {
						//释放锁
						redisDistributeLock.releaseLock(key, value);
					}
				}
			});
		}
	}
}
