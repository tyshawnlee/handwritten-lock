package org.tyshawn.lock.zookeeper;

import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于临时顺序节点的ZooKeeper分布式锁
 * 解决了羊群效应的问题和锁公平性的问题
 * @author litianxiang
 * @date 2020/3/12 16:08
 */
public class ZkDistributeLock {
	private static Logger logger = LoggerFactory.getLogger(ZkDistributeLock.class);

	/**
	 * 分布式锁的根节点路径
	 */
	private String rootLockPath = "/exclusive_lock";
	/**
	 * 分布式锁节点路径
	 */
	private String lockPath;
	/**
	 * 分布式锁名
	 */
	private String lockName;
	private ZooKeeper zk;

	/**
	 * 连接zk, 并创建分布式锁的根节点
	 * @param host zk服务地址
	 * @param lockName 分布式锁名
	 */
	public ZkDistributeLock(String host, String lockName) {
		try {
			CountDownLatch connectedSignal = new CountDownLatch(1);
			zk = new ZooKeeper(host, 5000, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					if (event.getState() == Event.KeeperState.SyncConnected) {
						connectedSignal.countDown();
					}
				}
			});
			//因为监听器是异步操作, 要保证监听器操作先完成, 即要确保先连接上ZooKeeper再返回实例.
			connectedSignal.await();

			//创建锁的根节点(持久节点)
			if (zk.exists(rootLockPath, false) == null) {
				zk.create(rootLockPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}

			//指定分布式锁节点路径
			this.lockName = lockName;
		} catch (Exception e) {
			logger.error("connect zookeeper server error.", e);
		}
	}

	/**
	 * 获取锁
	 * 在业务中获取到锁后才能继续往下执行, 否则堵塞, 直到获取到锁
	 */
	public void getLock() {
		try {
			//创建分布式锁的临时顺序节点
			lockPath = zk.create(rootLockPath + "/" + lockName, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

			//取出所有分布式锁的临时顺序节点, 然后排序
			List<String> children = zk.getChildren(rootLockPath, false);
			TreeSet<String> sortedChildren = new TreeSet<>();
			for (String child : children) {
				sortedChildren.add(rootLockPath + "/" + child);
			}

			//如果当前客户端创建的顺序节点是第一个, 则获取到锁
			String firstNode = sortedChildren.first();
			if (firstNode.equals(lockPath)) {
				return;
			}

			//如果当前客户端没有获取到锁, 则在前一个临时顺序节点上加一个监听器
			String lowerNode = sortedChildren.lower(lockPath);
			CountDownLatch latch = new CountDownLatch(1);
			if (StringUtils.isBlank(lowerNode)) {
				return;
			}
			Stat stat = zk.exists(lowerNode, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					//当前一个临时顺序节点被删除后, 当前客户端就获取到锁(这样就保证了锁的公平性)
					if (event.getType() == Event.EventType.NodeDeleted) {
						latch.countDown();
					}
				}
			});
			if (stat != null) {
				latch.await();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 释放锁
	 */
	public void releaseLock() {
		try {
			zk.delete(lockPath, -1);
		} catch (Exception e) {
			logger.error("release lock error.", e);
		}
	}

	public static void main(String[] args) {
		ExecutorService executorService = Executors.newFixedThreadPool(5);
		for (int i = 0; i < 5; i++) {
			executorService.execute(new Runnable() {
				@Override
				public void run() {
					ZkDistributeLock zkDistributeLock = new ZkDistributeLock("xxx.xx.xx.xxx:2181", "myLock");

					//获取锁, 没有获取到锁就一直等待
					zkDistributeLock.getLock();
					try {
						logger.info(Thread.currentThread().getName() + " 进行扣减库存操作...");
					} catch (Exception e) {
						logger.error("处理业务逻辑报错", e);
					}finally {
						//释放锁
						zkDistributeLock.releaseLock();
					}
				}
			});
		}
	}
}
