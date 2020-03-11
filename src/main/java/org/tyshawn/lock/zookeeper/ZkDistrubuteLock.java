package org.tyshawn.lock.zookeeper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkDistrubuteLock {
    private static Logger logger = LoggerFactory.getLogger(ZkDistrubuteLock.class);

    private String lockRootPath = "/exclusive_lock";
    private String tempLockPath = lockRootPath + "/lock";
    private final CountDownLatch connectedSignal = new CountDownLatch(1);
    private ZooKeeper zk;

    public ZkDistrubuteLock(String host) {
        try {
            zk = new ZooKeeper(host, 5000, new Watcher() {
                public void process(WatchedEvent event) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        connectedSignal.countDown();
                    }
                }
            });
            //因为监听器是异步操作, 要保证监听器操作先完成, 即要确保先连接上ZooKeeper再返回实例.
            connectedSignal.await();

            //创建锁的根节点(持久节点)
            if (zk.exists(lockRootPath, false) == null) {
                zk.create(lockRootPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            logger.error("connect zookeeper server error.");
        }

    }


    private void watchNode(final String nodePath, final Thread thread) throws InterruptedException {
        try {
            zk.exists(nodePath, new Watcher() {
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeDeleted) {
                        logger.info("lock release.");
                        thread.interrupt();
                    }
                    try {
                        zk.exists(nodePath, true);
                    } catch (InterruptedException e) {
                        logger.error(e.getMessage(), e);
                    } catch (KeeperException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            });
        }catch (KeeperException e){
            logger.error("watch node error.", e);
        }
    }

    /**
     * 获取锁
     * @return
     * @throws InterruptedException
     */
    public boolean getLock() throws InterruptedException {
        createLockRootPath();
        watchNode(tempLockPath, Thread.currentThread());

        String path = null;
        while (true) {
            try {
                path = zk.create(tempLockPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException e) {
                logger.info(Thread.currentThread().getName() + " get lock fail.");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                    logger.info(Thread.currentThread().getName() + " notify.");
                }
            }
            if (StringUtils.isNotBlank(path)) {
                logger.info(Thread.currentThread().getName() + " get lock.");
                return true;
            }
        }
    }


    /**
     * 释放锁
     */
    public void releaseLock() {
        try {
            zk.delete(tempLockPath, -1);
            logger.info(Thread.currentThread().getName() + " release lock.");
        } catch (InterruptedException e) {
            logger.error("release lock error.", e);
        } catch (KeeperException e) {
            logger.error("release lock error.", e);
        }
    }


    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 5; i++) {
            executorService.execute(new Runnable() {
                public void run() {
                    ZkDistrubuteLock lock = new ZkDistrubuteLock("114.55.27.138:2181");
                    try {
                        lock.getLock();
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    lock.releaseLock();
                }
            });
        }
        executorService.shutdown();
    }
}
